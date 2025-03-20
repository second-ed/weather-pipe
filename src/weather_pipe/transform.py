import uuid
from copy import deepcopy
from datetime import datetime

import attrs
import polars as pl
from returns.result import Failure, Result, Success, safe

from weather_pipe.data_structures import (
    CleanedTables,
    EncodedTables,
    NormalisedTables,
    RawTables,
    UnnestedTables,
)
from weather_pipe.utils import log_call


@log_call
def convert_json_to_df(data: dict, table_path: list) -> Result[pl.DataFrame, Exception]:
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
        return Success(pl.DataFrame(data))
    except Exception as e:
        return Failure({"err": str(e)})


@log_call
def add_ingestion_columns(
    df: pl.DataFrame, batch_guid: str, date_time: datetime
) -> Result[pl.DataFrame, Exception]:
    try:
        df = df.with_columns(
            pl.Series("row_guid", [str(uuid.uuid4()) for _ in range(len(df))]),
            pl.lit(batch_guid).alias("batch_guid"),
            pl.lit(date_time).alias("ingestion_datetime"),
        )
        return Success(df)
    except Exception as e:
        return Failure({"err": str(e)})


@log_call
@safe
def unnest_struct_cols(raw_tables: RawTables) -> UnnestedTables:
    unnested_tables = UnnestedTables(*attrs.astuple(raw_tables))
    unnested_tables.fact_table = unnested_tables.fact_table.unnest(
        [
            col
            for col in unnested_tables.fact_table.columns
            if unnested_tables.fact_table.schema[col] == pl.Struct
        ]
    )
    return unnested_tables


@log_call
@safe
def clean_text_cols(unnested_tables: UnnestedTables) -> CleanedTables:
    cleaned_tables = CleanedTables(*attrs.astuple(unnested_tables))
    cleaned_tables.fact_table = cleaned_tables.fact_table.with_columns(
        [
            pl.col(col).str.strip_chars().str.to_lowercase()
            for col in cleaned_tables.fact_table.columns
            if cleaned_tables.fact_table.schema[col] == pl.Utf8
        ]
    )
    return cleaned_tables


@log_call
@safe
def normalise_table(cleaned_tables: CleanedTables) -> NormalisedTables:
    norm_tables = NormalisedTables(*attrs.astuple(cleaned_tables))
    norm_tables.dim_tables = {}
    for col in norm_tables.cols:
        norm_table = norm_tables.fact_table[col].unique().to_frame()
        norm_table = norm_table.with_columns(
            pl.arange(0, norm_table.height).alias(f"{col}_id")
        )
        norm_tables.dim_tables[col] = norm_table.select(f"{col}_id", col)
    return norm_tables


@log_call
@safe
def replace_col_with_id(norm_tables: NormalisedTables) -> EncodedTables:
    encoded_tables = EncodedTables(*attrs.astuple(norm_tables))
    for col, dim in encoded_tables.dim_tables.items():
        encoded_tables.fact_table = encoded_tables.fact_table.join(
            dim, on=col, how="left"
        ).drop(col)
    return encoded_tables
