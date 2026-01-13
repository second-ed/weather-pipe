from __future__ import annotations

import re
import uuid
from copy import deepcopy
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
from danom import safe

if TYPE_CHECKING:
    import datetime as dt

    from weather_pipe.v2.layers.raw.data_structures import ApiConfig


@safe
def convert_json_to_df(data: dict, table_path: list) -> pl.DataFrame:
    data = deepcopy(data)
    for level in table_path:
        if isinstance(data, dict):
            data = data.get(level)
            continue
        if isinstance(data, list):
            if len(data) > 0:
                data = data[level]
                continue
            return {"err": "invalid level given for list"}
    return pl.DataFrame(data)


@safe
def add_ingestion_columns(
    df: pl.DataFrame,
    location: str,
    batch_guid: str,
    date_time: str,
) -> pl.DataFrame:
    return df.with_columns(
        pl.lit(location).alias("location"),
        pl.Series("sys_col_row_guid", [str(uuid.uuid4()) for _ in range(len(df))]),
        pl.lit(batch_guid).alias("sys_col_batch_guid"),
        pl.lit(date_time).alias("sys_col_ingestion_datetime"),
    )


def clean_str(inp_str: str) -> str:
    return re.sub(r"_+", "_", re.sub(r"[^a-zA-Z0-9]", "_", inp_str)).lower()


def fmt_time(date_time: dt.datetime, date_fmt: str = "%Y%m%d_%H%M%S") -> str:
    return date_time.strftime(date_fmt)


def generate_raw_save_path(
    api_config: ApiConfig,
    repo_root: str,
    save_dir: str,
    start_time: dt.datetime,
) -> str:
    cleaned_location = clean_str(api_config.location)
    return (
        Path(repo_root)
        / save_dir
        / cleaned_location
        / f"{fmt_time(start_time)}_{api_config.request_type}_{cleaned_location}.parquet"
    )
