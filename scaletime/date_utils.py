from datetime import date, timedelta
from typing import Callable, Iterator

from pandas.tseries.holiday import USFederalHolidayCalendar


def is_weekend(ts: date) -> bool:
    return ts.weekday() >= 5


def is_friday(ts: date) -> bool:
    return ts.weekday() == 4


def closest_friday(ts: date) -> date:
    cts = ts + timedelta(days=1)
    while not is_friday(cts):
        cts += timedelta(days=1)
    return cts


def fridays(ts: date) -> Iterator[date]:
    cts = ts
    while True:
        cts = closest_friday(cts)
        yield cts


def is_holiday_fn(start: date, end: date) -> Callable[[date], bool]:
    cal = USFederalHolidayCalendar().holidays(start, end)  # type: ignore
    holidays = {h.date() for h in cal}
    return lambda d: d in holidays
