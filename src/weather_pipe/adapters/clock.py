import datetime as dt


def clock_now() -> str:
    return dt.datetime.now(dt.UTC)


def fake_clock_now() -> str:
    return dt.datetime(2025, 5, 27, 19, 40, 00, tzinfo=dt.UTC)


def fmt_time(date_time: dt.datetime, date_fmt: str = "%Y%m%d_%H%M%S") -> str:
    return date_time.strftime(date_fmt)
