import os
from enum import Enum, auto

import polars as pl
import yaml
from returns.result import safe


class FileType(Enum):
    PARQUET = auto()
    YAML = auto()
    SQLITE = auto()


@safe
def read_yaml(path: str) -> dict:
    with open(path, "r") as f:
        data = yaml.safe_load(f)
    return data


@safe
def write_yaml(data: dict, path: str) -> bool:
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, sort_keys=False, allow_unicode=True)
    return True


@safe
def read_parquet(path: str) -> pl.DataFrame:
    return pl.read_parquet(path)


@safe
def write_parquet(data: pl.DataFrame, path: str) -> bool:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    data.write_parquet(path)
    return True


@safe
def read_sqlite(
    path: str,
    connection: pl._typing.ConnectionOrCursor,
    execute_options: dict | None = None,
):
    return pl.read_database(
        query=path, connection=connection, execute_options=execute_options
    )


@safe
def write_sqlite(
    data: pl.DataFrame,
    path: str,
    connection: pl._typing.ConnectionOrCursor,
    if_table_exists: str = "fail",
    engine_options: dict | None = None,
):
    data.write_database(
        table_name=path,
        connection=connection,
        if_table_exists=if_table_exists,
        engine_options=engine_options,
    )
    return True


IO_READERS = {
    FileType.PARQUET: read_parquet,
    FileType.YAML: read_yaml,
    FileType.SQLITE: read_sqlite,
}

IO_WRITERS = {
    FileType.PARQUET: write_parquet,
    FileType.YAML: write_yaml,
    FileType.SQLITE: write_sqlite,
}
