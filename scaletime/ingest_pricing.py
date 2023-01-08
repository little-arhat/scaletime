import math
import random

from datetime import date, datetime, time, timedelta
from itertools import filterfalse, islice
from typing import Iterator, List, Tuple
from zoneinfo import ZoneInfo

import pandas as pd

from tqdm import tqdm

from . import args, date_utils, db, defs


def calculate_ttes(
    ts: datetime, expiry: date, expiry_time: time
) -> Tuple[float, float]:
    edt = datetime.combine(expiry, expiry_time, tzinfo=ts.tzinfo)
    to_expiry = edt - ts
    ttexp = to_expiry.total_seconds() / defs.SECONDS_IN_A_YEAR
    vtexp = (
        to_expiry.seconds / defs.BUSINESS_SECONDS_IN_A_YEAR
    ) * random.uniform(0.9, 1.2)
    return (ttexp, vtexp)


def generate_for_ts(
    ts: datetime, fund: str, expiries: List[date], num_strikes: int
) -> Iterator[List[object]]:
    base_price = random.uniform(3000, 3500)
    for (i, expiry) in enumerate(expiries):
        forward = base_price * (1 + i * 0.1)
        base_volatility = random.uniform(10, 100)
        ttexp, vtexp = calculate_ttes(ts, expiry, defs.EXPIRY_TIME)
        additional_event_variance = random.uniform(0, 1)
        rho = random.random()
        for strike in range(100, 5 * num_strikes + 1, 5):
            moneyness = math.log(forward / strike) / math.sqrt(
                max(vtexp, defs.MIN_VTEXP)
            )
            bv_moneyness = moneyness / base_volatility
            for kind in defs.OPTION_KINDS:
                yield [
                    ts,
                    fund,
                    expiry,
                    defs.EXPIRY_TIME,
                    "WEEKLY",
                    float(strike),
                    random.uniform(50, 250),  # price
                    base_volatility + random.uniform(-5, 45),
                    random.uniform(0, 1),
                    random.uniform(0, 1),
                    random.uniform(0, 1),
                    random.uniform(0, 1),
                    random.uniform(0, 1),
                    additional_event_variance,
                    forward,
                    base_price,
                    rho,
                    random.uniform(0, 1),
                    random.uniform(0, 1),
                    random.uniform(0, 1),
                    random.uniform(0, 1),
                    random.uniform(0, 1),
                    random.uniform(0, 1),
                    ttexp,
                    vtexp,
                    moneyness,
                    base_volatility,
                    bv_moneyness,
                    random.uniform(0, 10),
                    random.uniform(0, 1),
                    random.uniform(0, 1),
                    random.uniform(0, 0.2),
                    random.uniform(0, 0.2),
                    "Option",
                    kind,
                ]


def run(
    conn: db.Db,
    fund: str,
    start: date,
    end: date,
    mkt_open: time,
    mkt_close: time,
    freq: pd.Timedelta,
    tz: ZoneInfo,
    num_expiries: int,
    num_strikes: int,
) -> None:
    is_holiday = date_utils.is_holiday_fn(start, end)

    current_date = start
    while current_date <= end:
        if is_holiday(current_date) or date_utils.is_weekend(current_date):
            current_date += timedelta(days=1)
            continue

        expiries = list(
            islice(
                filterfalse(is_holiday, date_utils.fridays(current_date)),
                num_expiries,
            )
        )
        current_ts = datetime.combine(current_date, mkt_open, tzinfo=tz)
        end_current_date = datetime.combine(current_date, mkt_close, tzinfo=tz)
        iterations = (end_current_date - current_ts).seconds // freq.seconds
        print(f"Working with {current_date} [now={datetime.now()}]:")
        with tqdm(total=iterations) as pbar:
            while current_ts <= end_current_date:
                pbar.set_description(f"Processing: {current_ts}")
                try:
                    conn.bulk_insert(
                        defs.PRICING_TABLE,
                        defs.PRICING_COLUMNS,
                        generate_for_ts(
                            current_ts, fund, expiries, num_strikes
                        ),
                    )
                except Exception as e:
                    print(f"Error inserting data for {current_ts}: {e}")
                current_ts += freq
                pbar.update(1)
        try:
            conn.commit()
        except Exception as e:
            print(f"Failed to commit {current_date}: {e}")
        print(f"Done with {current_date}; [now={datetime.now()}]!")
        current_date += timedelta(days=1)


def main() -> None:
    parser = args.create_arg_parser()
    parser.add_argument("--fund", required=True, type=str)
    parser.add_argument("--start", required=True, type=date.fromisoformat)
    parser.add_argument("--end", required=True, type=date.fromisoformat)
    parser.add_argument("--open", type=time.fromisoformat, default=time(8, 30))
    parser.add_argument("--close", type=time.fromisoformat, default=time(15, 0))
    parser.add_argument(
        "--freq", type=pd.Timedelta, default=pd.Timedelta("5min")
    )
    parser.add_argument("--num-expiries", type=int, default=12)
    parser.add_argument("--num-strikes", type=int, default=500)
    arguments = parser.parse_args()

    with db.connect(arguments) as conn:
        run(
            conn,
            arguments.fund,
            arguments.start,
            arguments.end,
            arguments.open,
            arguments.close,
            arguments.freq,
            arguments.tz,
            arguments.num_expiries,
            arguments.num_strikes,
        )
