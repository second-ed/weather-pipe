import datetime as dt


def clock_now(date_fmt: str = "%Y%m%d_%H%M%S") -> str:
    return dt.datetime.now(dt.UTC).strftime(date_fmt)


def fake_clock_now(date_fmt: str = "%Y%m%d_%H%M%S") -> str:
    return dt.datetime(2025, 5, 27, 19, 40, 00, tzinfo=dt.UTC).strftime(date_fmt)
