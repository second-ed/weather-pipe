from collections import defaultdict
from typing import Any, Protocol, TypeVar, runtime_checkable

import attrs
import polars as pl
import requests
from returns.result import Failure, Result, Success

import weather_pipe.io_funcs as iof
from weather_pipe.data_structures import ApiConfig

Data = TypeVar("Data")


@runtime_checkable
class IOWrapperProtocol(Protocol):
    def read(self, path: str, file_type: iof.FileType) -> Result[Data, Exception]: ...

    def write(
        self, data, path: str, file_type: iof.FileType
    ) -> Result[bool, Exception]: ...

    def extract_data(
        self, api_config: ApiConfig
    ) -> Result[dict[str, Any], Exception]: ...


@attrs.define
class LocalIOWrapper:
    def read(self, path: str, file_type: iof.FileType) -> Result[Data, Exception]:
        return iof.IO_READERS[file_type](path)

    def write(
        self, data: dict | pl.DataFrame, path: str, file_type: iof.FileType
    ) -> Result[bool, Exception]:
        return iof.IO_WRITERS[file_type](data, path)

    def extract_data(self, api_config: ApiConfig) -> Result[dict[str, Any], Exception]:
        call = f"http://api.weatherapi.com/v1/{api_config.request_type}.json?key={api_config.api_key}&q={api_config.location}&aqi=no"
        response = requests.get(call, timeout=10)

        if response.status_code == 200:
            return Success(response.json())
        return Failure(response)


@attrs.define
class FakeLocalIOWrapper:
    db: defaultdict = attrs.field(default=None)
    external_src: dict = attrs.field(default=attrs.Factory(dict))
    log: list = attrs.field(default=attrs.Factory(list))

    def __attrs_post_init__(self):
        self.db = self.db or defaultdict(dict)

    def read(self, path: str, file_type: iof.FileType) -> Result[Data, Exception]:
        self.log.append({"func": "read", "path": path, "file_type": file_type})
        try:
            return Success(self.db[file_type][path])
        except KeyError as e:
            return Failure({"err": str(e), "path": path})

    def write(
        self, data: dict | pl.DataFrame, path: str, file_type: iof.FileType
    ) -> Result[bool, Exception]:
        self.log.append({"func": "write", "path": path, "file_type": file_type})
        self.db[file_type][path] = data
        return Success(True)

    def extract_data(self, api_config: ApiConfig) -> Result[dict[str, Any], Exception]:
        return Success(self.external_src[api_config.location])
