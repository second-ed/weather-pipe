import re
import uuid
from copy import deepcopy

import polars as pl
from returns.result import safe


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
    batch_guid: str,
    date_time: str,
) -> pl.DataFrame:
    return df.with_columns(
        pl.Series("row_guid", [str(uuid.uuid4()) for _ in range(len(df))]),
        pl.lit(batch_guid).alias("batch_guid"),
        pl.lit(date_time).alias("ingestion_datetime"),
    )


def clean_str(inp_str: str) -> str:
    return re.sub(r"_+", "_", re.sub(r"[^a-zA-Z0-9]", "_", inp_str)).lower()
