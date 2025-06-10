from enum import Enum, auto
from typing import Any, Protocol, runtime_checkable

import attrs
import polars as pl
from returns.result import Result, Success, safe

from weather_pipe.domain.data_structures import ApiConfig

Data = pl.DataFrame | dict


class FileType(Enum):
    PARQUET = auto()
    CSV = auto()
    JSON = auto()
    SQLITE3 = auto()
    YAML = auto()


@runtime_checkable
class IOWrapperProtocol(Protocol):
    def setup(self) -> bool: ...

    def teardown(self) -> bool: ...

    def read(self, path: str, file_type: FileType, **kwargs: dict) -> Data: ...

    def write(self, path: str, data: Data, file_type: FileType, **kwargs: dict) -> bool: ...

    def extract_data(self, api_config: ApiConfig) -> Result[dict[str, Any], Exception]: ...


@attrs.define
class FakeIOWrapper:
    db: dict = attrs.field(default=attrs.Factory(dict))
    db_name: str = attrs.field(default="")
    log: list = attrs.field(default=attrs.Factory(list))
    external_src: dict = attrs.field(default=attrs.Factory(dict))

    def setup(self) -> bool:
        return True

    def teardown(self) -> bool:
        return True

    @safe
    def read(self, path: str, file_type: FileType, **kwargs: dict) -> Data:
        self.log.append({"func": "read", "path": path, "file_type": file_type, "kwargs": kwargs})
        return self.db[file_type][path]

    @safe
    def write(self, data: Data, path: str, file_type: FileType, **kwargs: dict) -> bool:
        self.log.append({"func": "write", "path": path, "file_type": file_type, "kwargs": kwargs})
        self.db[file_type][path] = data
        return True

    def extract_data(self, api_config: ApiConfig) -> Result[dict[str, Any], Exception]:
        return Success(self.external_src[api_config.location])
