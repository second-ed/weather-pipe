import re
import uuid
from copy import deepcopy

import attrs
import polars as pl
from returns.result import safe

from weather_pipe.domain.data_structures import CleanedTable, EncodedTable, RawTable, UnnestedTable


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


@safe
def unnest_struct_cols(raw_tables: RawTable) -> UnnestedTable:
    unnested_table = UnnestedTable(*attrs.astuple(raw_tables))
    unnested_table.table = unnested_table.table.unnest(
        [
            col
            for col in unnested_table.table.columns
            if unnested_table.table.schema[col] == pl.Struct
        ],
    )
    return unnested_table


@safe
def clean_text_cols(unnested_table: UnnestedTable) -> CleanedTable:
    cleaned_table = CleanedTable(*attrs.astuple(unnested_table))
    cleaned_table.table = cleaned_table.table.with_columns(
        [
            pl.col(col).str.strip_chars().str.to_lowercase()
            for col in cleaned_table.table.columns
            if cleaned_table.table.schema[col] == pl.Utf8
        ],
    )
    return cleaned_table


@safe
def replace_col_with_id(
    cleaned_table: CleanedTable,
    dim_tables: dict[str, pl.DataFrame],
) -> EncodedTable:
    encoded_tables = EncodedTable(*attrs.astuple(cleaned_table))
    for col, dim in dim_tables.items():
        encoded_tables.table = encoded_tables.table.join(
            dim,
            on=col,
            how="left",
        ).drop(col)
    return encoded_tables
