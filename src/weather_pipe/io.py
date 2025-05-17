import os
from typing import Any, Protocol, runtime_checkable

import attrs
import polars as pl
import requests
import yaml
from returns.result import Failure, Result, Success

from weather_pipe.data_structures import ApiConfig


@runtime_checkable
class IOWrapperProtocol(Protocol):
    def read_yaml(self, path: str) -> Result[dict, Exception]: ...

    def write_parquet(self, df: pl.DataFrame, path: str) -> Result[bool, Exception]: ...

    def extract_data(
        self, api_config: ApiConfig
    ) -> Result[dict[str, Any], Exception]: ...


@attrs.define
class IOWrapper:
    def read_yaml(self, path: str) -> Result[dict, Exception]:
        try:
            with open(path, "r") as f:
                data = yaml.safe_load(f)
            if data is None:
                return Failure({"err": "empty yaml", "path": path})

            return Success(data)
        except Exception as e:
            return Failure({"err": str(e), "path": path})

    def write_parquet(self, df: pl.DataFrame, path: str) -> Result[bool, Exception]:
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            df.write_parquet(path)
            return Success(True)
        except Exception as e:
            return Failure({"err": str(e)})

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

    def read_yaml(self, path: str) -> Result[dict, Exception]:
        self.log.append({"func": "read_yaml", "path": path})
        try:
            return Success(self.db[path])
        except KeyError as e:
            return Failure({"err": str(e), "path": path})

    def write_parquet(self, df: pl.DataFrame, path: str) -> Result[bool, Exception]:
        self.log.append({"func": "write_parquet", "path": path})
        self.db[path] = df
        return Success(True)

    def extract_data(self, api_config: ApiConfig) -> Result[dict[str, Any], Exception]:
        return Success(self.external_src[api_config.location])
