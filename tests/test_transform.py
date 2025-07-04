import datetime as dt

import polars as pl
import pytest
from hypothesis import given
from hypothesis.strategies import text
from returns.result import Failure, Success

from weather_pipe.domain.transform import add_ingestion_columns, clean_str


@pytest.mark.parametrize(
    ("df", "batch_guid", "date_time", "err"),
    [
        pytest.param(
            pl.DataFrame({"col_1": [1, 2, 3, 4, 5, 6], "col_2": ["A", "B", "C", "D", "E", "F"]}),
            "123-abc",
            dt.datetime(2000, 1, 1, tzinfo=dt.timezone.utc),
            None,
        ),
        pytest.param(
            [],
            "123-abc",
            dt.datetime(2000, 1, 1, tzinfo=dt.timezone.utc),
            ("'list' object has no attribute 'with_columns'",),
        ),
    ],
)
def test_add_ingestion_columns(df, batch_guid, date_time, err):
    result = add_ingestion_columns(df, batch_guid, date_time)

    match result:
        case Success(inner):
            assert inner["row_guid"].n_unique() == len(inner)
            assert inner["batch_guid"].eq(batch_guid).all()
            assert inner["ingestion_datetime"].eq(date_time).all()

        case Failure(inner):
            assert inner.args == err


@given(text())
def test_clean_str(input_url):
    result = clean_str(input_url)
    assert "__" not in result
    assert result == result.lower()
    assert all(char.isalnum() for char in result if char != "_")
