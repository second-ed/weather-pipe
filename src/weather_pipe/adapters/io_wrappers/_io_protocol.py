import sqlite3
from enum import Enum
from typing import Protocol, runtime_checkable

import attrs
import polars as pl

from weather_pipe.domain.data_structures import ApiConfig
from weather_pipe.domain.result import Ok, Result, safe

Data = pl.DataFrame | dict


class FileType(Enum):
    PARQUET = ".parquet"
    CSV = ".csv"
    JSON = ".json"
    SQLITE3 = ".db"
    YAML = ".yaml"


@runtime_checkable
class IOWrapperProtocol(Protocol):
    def setup(self) -> bool: ...

    def teardown(self) -> bool: ...

    def read(self, path: str, file_type: FileType, **kwargs: dict) -> Data: ...

    def write(self, data: Data, path: str, file_type: FileType, **kwargs: dict) -> bool: ...

    def extract_data(self, api_config: ApiConfig) -> Result: ...


@attrs.define
class FakeIOWrapper:
    db: dict = attrs.field(default=attrs.Factory(dict))
    db_name: str = attrs.field(default="")
    log: list = attrs.field(default=attrs.Factory(list))
    uri: str = attrs.field(default="")
    conn: sqlite3.Connection | None = attrs.field(default=None)  # noqa: FA102
    external_src: dict = attrs.field(default=attrs.Factory(dict))

    def setup(self) -> bool:
        if self.db_name:
            self.conn = sqlite3.connect(self.db_name)
            self.uri = f"sqlite:///{self.db_name}"
        return True

    def teardown(self) -> bool:
        return True

    @safe
    def read(self, path: str, file_type: FileType, **kwargs: dict) -> Data:
        self.log.append({"func": "read", "path": path, "file_type": file_type, "kwargs": kwargs})
        match file_type:
            case FileType.SQLITE3:
                return pl.read_database_uri(query=path, uri=self.uri, engine="connectorx", **kwargs)
            case _:
                return self.db[path]

    @safe
    def write(self, data: Data, path: str, file_type: FileType, **kwargs: dict) -> bool:
        self.log.append({"func": "write", "path": path, "file_type": file_type, "kwargs": kwargs})
        match file_type:
            case FileType.SQLITE3:
                data.write_database(
                    table_name=path,
                    connection=self.uri,
                    engine="sqlalchemy",
                    **kwargs,
                )
            case _:
                self.db[path] = data
        return True

    def extract_data(self, api_config: ApiConfig) -> Result:
        return Ok(self.external_src[api_config.location])
