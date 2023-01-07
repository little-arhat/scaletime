import argparse
import math
import random

from datetime import date, datetime, time, timedelta
from io import BytesIO
from itertools import filterfalse, islice
from typing import Iterator, List
from zoneinfo import ZoneInfo

import pandas as pd
import psycopg2

from pgcopy import CopyManager  # type: ignore
from tqdm import tqdm

from . import date_utils


DEFAULT_EXPIRY_TIME = time(15, 30)
MIN_TTE = 0.001


PRICING_COLUMNS = [
    "time",
    "fund",
    "expiry_date",
    "expiry_time",
    "expiry_cycle",
    "strike",
    "price",
    "volatility",
    "delta",
    "skew_delta",
    "gamma",
    "skew_gamma",
    "theta",
    "additional_event_variance",
    "forward",
    "base_price",
    "rho",
    "moving_skew",
    "moving_smile",
    "underlying_delta",
    "underlying_skew_delta",
    "vega",
    "tw_vega",
    "time_to_expiry",
    "vola_time_to_expiry",
    "strike_moneyness",
    "base_volatility",
    "basevol_moneyness",
    "market_width",
    "vanna",
    "charm",
    "price_error",
    "vola_error",
    "contract_type",
    "option_kind",
]


def generate_for_ts(
    ts: datetime, fund: str, expiries: List[date], num_strikes: int
) -> Iterator[List[object]]:
    base_price = random.uniform(3000, 3500)
    for (i, expiry) in enumerate(expiries):
        forward = base_price * (1 + i * 0.1)
        base_volatility = random.uniform(10, 100)
        days_to_exp = (expiry - ts.date()).days
        tte = days_to_exp / 365.0
        vtexp = days_to_exp / 252.0
        additional_event_variance = random.uniform(0, 1)
        rho = random.random()
        for strike in range(100, 5 * num_strikes + 1, 5):
            moneyness = math.log(forward / strike) / math.sqrt(
                max(vtexp, MIN_TTE)
            )
            bv_moneyness = moneyness / base_volatility
            for kind in ["call", "put"]:
                yield [
                    ts,
                    fund,
                    expiry,
                    DEFAULT_EXPIRY_TIME,
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
                    tte,
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
    conn: psycopg2.extensions.connection,
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
    mgr = CopyManager(conn, "raw_pricing", PRICING_COLUMNS)
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
        print(f"Working with {current_date}:")
        with tqdm(total=iterations) as pbar:
            while current_ts <= end_current_date:
                pbar.set_description(f"Processing: {current_ts}")
                try:
                    mgr.copy(
                        generate_for_ts(
                            current_ts, fund, expiries, num_strikes
                        ),
                        BytesIO,
                    )
                except Exception as e:
                    print(f"Error dumping {current_ts}: {e}")
                current_ts += freq
                pbar.update(1)
        try:
            conn.commit()
        except Exception as e:
            print(f"Failed to commit {current_date}: {e}")
        print(f"Done with {current_date}!")
        current_date += timedelta(days=1)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fund", required=True, type=str)
    parser.add_argument("--start", required=True, type=date.fromisoformat)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--open", type=time.fromisoformat, default=time(8, 30))
    parser.add_argument("--close", type=time.fromisoformat, default=time(15, 0))
    parser.add_argument(
        "--freq", type=pd.Timedelta, default=pd.Timedelta("5min")
    )
    parser.add_argument(
        "--tz", type=ZoneInfo, default=ZoneInfo("America/Chicago")
    )
    parser.add_argument("--num-expiries", type=int, default=12)
    parser.add_argument("--num-strikes", type=int, default=500)
    parser.add_argument("--user", type=str, required=True)
    parser.add_argument("--pass", type=str, required=True)
    parser.add_argument("--host", type=str, required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--dbname", type=str, required=True)
    arguments = parser.parse_args()
    connection = "postgres://{user}:{pass}@{host}:{port}/{dbname}?sslmode=require".format(
        **vars(arguments)
    )
    with psycopg2.connect(connection) as conn:
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
