import datetime as dt

import polars as pl
import pytest
from hypothesis import given
from hypothesis.strategies import text
from returns.result import Failure, Success

import weather_pipe.domain.data_structures as ds
import weather_pipe.domain.transform as tf


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
    result = tf.add_ingestion_columns(df, batch_guid, date_time)

    match result:
        case Success(inner):
            assert inner["row_guid"].n_unique() == len(inner)
            assert inner["batch_guid"].eq(batch_guid).all()
            assert inner["ingestion_datetime"].eq(date_time).all()

        case Failure(inner):
            assert inner.args == err


@given(text())
def test_clean_str(input_url):
    result = tf.clean_str(input_url)
    assert "__" not in result
    assert result == result.lower()
    assert all(char.isalnum() for char in result if char != "_")


@pytest.mark.parametrize(
    ("df", "cols", "expected_fact_table", "expected_dim_table"),
    [
        pytest.param(
            pl.DataFrame(
                {
                    "name": ["Alice", "Bob", "Charlie"],
                    "info": pl.Series(
                        [
                            {"age": 25, "city": "London"},
                            {"age": 30, "city": "Paris"},
                            {"age": 22, "city": "Berlin"},
                        ],
                    ),
                },
            ),
            ["city"],
            [
                {"name": "alice", "age": 25, "city_id": 1},
                {"name": "bob", "age": 30, "city_id": 2},
                {"name": "charlie", "age": 22, "city_id": 0},
            ],
            [
                {"city_id": 0, "city": "berlin"},
                {"city_id": 1, "city": "london"},
                {"city_id": 2, "city": "paris"},
            ],
        ),
    ],
)
def test_transform_tables(df, cols, expected_fact_table, expected_dim_table):
    layer_tables = ds.RawTables(fact_table=df, cols=cols)
    res = (
        tf.unnest_struct_cols(layer_tables)
        .bind(tf.clean_text_cols)
        .bind(tf.normalise_table)
        .bind(tf.replace_col_with_id)
    )

    assert isinstance(res, Success)
    encoded_tables = res.unwrap()
    assert encoded_tables.fact_table.to_dicts() == expected_fact_table
    assert encoded_tables.dim_tables[cols[0]].to_dicts() == expected_dim_table
