from datetime import datetime

import polars as pl
import pytest
from returns.result import Failure, Success

from src.weather_pipe.transform import add_ingestion_columns


@pytest.mark.parametrize(
    "df, batch_guid, date_time, err",
    (
        pytest.param(
            pl.DataFrame(
                {"col_1": [1, 2, 3, 4, 5, 6], "col_2": ["A", "B", "C", "D", "E", "F"]}
            ),
            "123-abc",
            datetime(2000, 1, 1),
            None,
        ),
        pytest.param(
            [],
            "123-abc",
            datetime(2000, 1, 1),
            {"err": "'list' object has no attribute 'with_columns'"},
        ),
    ),
)
def test_add_ingestion_columns(df, batch_guid, date_time, err):
    result = add_ingestion_columns(df, batch_guid, date_time)

    match result:
        case Success(inner):
            assert inner["row_guid"].n_unique() == len(inner)
            assert inner["batch_guid"].eq(batch_guid).all()
            assert inner["ingestion_datetime"].eq(date_time).all()

        case Failure(inner):
            assert inner == err
