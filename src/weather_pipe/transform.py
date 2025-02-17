import uuid
from copy import deepcopy
from datetime import datetime
from typing import Any

import polars as pl
from returns.result import Failure, Result, Success


def get_table_from_json(data: dict, table_path: list) -> Result[dict[str, Any], Exception]:
    data = deepcopy(data)
    try:
        for level in table_path:
            if isinstance(data, dict):
                data = data.get(level)
                continue
            if isinstance(data, list):
                if len(data) > 0:
                    data = data[level]
                    continue
                else:
                    return Failure({"err": "invalid level given for list"})
        return Success(data)
    except Exception as e:
        return Failure({"err": str(e)})


def add_ingestion_columns(data: dict, batch_guid: str, date_time: datetime) -> Result[pl.DataFrame, Exception]:
    try:
        df = pl.DataFrame(data)
        df = df.with_columns(
            pl.Series("row_guid", [str(uuid.uuid4()) for _ in range(len(df))]),
            pl.lit(batch_guid).alias("batch_guid"),
            pl.lit(date_time).alias("ingestion_datetime"),
        )
        return Success(df)
    except Exception as e:
        return Failure({"err": str(e)})


