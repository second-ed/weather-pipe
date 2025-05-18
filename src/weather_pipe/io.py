from typing import Any, Protocol, runtime_checkable

import attrs
import polars as pl
import requests
from returns.result import Failure, Result, Success

import weather_pipe.io_funcs as iof
from weather_pipe.data_structures import ApiConfig


@runtime_checkable
class IOWrapperProtocol(Protocol):
    def read(self, path: str, file_type: iof.FileType) -> Result[dict, Exception]: ...

    def write(
        self, data, path: str, file_type: iof.FileType
    ) -> Result[bool, Exception]: ...

    def extract_data(
        self, api_config: ApiConfig
    ) -> Result[dict[str, Any], Exception]: ...


@attrs.define
class IOWrapper:
    def read(self, path: str, file_type: iof.FileType) -> Result[dict, Exception]:
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
class FakeIOWrapper:
    db: dict = attrs.field(default=attrs.Factory(dict))
    external_src: dict = attrs.field(default=attrs.Factory(dict))
    log: list = attrs.field(default=attrs.Factory(list))

    def read(self, path: str, file_type: iof.FileType) -> Result[dict, Exception]:
        self.log.append({"func": "read", "path": path, "file_type": file_type})
        try:
            return Success(self.db[path])
        except KeyError as e:
            return Failure({"err": str(e), "path": path})

    def write(
        self, data: dict | pl.DataFrame, path: str, file_type: iof.FileType
    ) -> Result[bool, Exception]:
        self.log.append({"func": "write", "path": path, "file_type": file_type})
        self.db[path] = data
        return Success(True)

    def extract_data(self, api_config: ApiConfig) -> Result[dict[str, Any], Exception]:
        return Success(self.external_src[api_config.location])
