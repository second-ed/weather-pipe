import os
import sqlite3
from typing import Any

import attrs
import polars as pl
import requests
import yaml
from returns.result import Failure, Result, Success, safe

from weather_pipe.adapters.io_wrappers._io_protocol import Data, FileType
from weather_pipe.domain.data_structures import ApiConfig


@attrs.define
class PolarsIO:
    db_name: str = attrs.field(default="")
    uri: str = attrs.field(default="")
    conn: sqlite3.Connection | None = attrs.field(default=None)  # noqa: FA102

    def setup(self) -> bool:
        if self.db_name:
            os.makedirs(os.path.dirname(self.db_name), exist_ok=True)
            self.conn = sqlite3.connect(self.db_name)
            self.uri = f"sqlite:///{self.db_name}"
        return True

    def teardown(self) -> bool:
        if self.conn is not None:
            self.conn.close()
        return True

    @safe
    def read(self, path: str, file_type: FileType, **kwargs: dict) -> Data:
        match file_type:
            case FileType.CSV:
                return pl.read_csv(path, **kwargs)
            case FileType.JSON:
                return pl.read_json(path, **kwargs)
            case FileType.PARQUET:
                return pl.read_parquet(path, **kwargs)
            case FileType.SQLITE3:
                return pl.read_database_uri(query=path, uri=self.uri, engine="connectorx", **kwargs)
            case FileType.YAML:
                with open(path) as f:
                    return yaml.safe_load(f)
            case _:
                raise NotImplementedError(f"`{file_type}` is not implemented")
        return True

    @safe
    def write(self, data: Data, path: str, file_type: FileType, **kwargs: dict) -> bool:
        if file_type != FileType.SQLITE3:
            os.makedirs(os.path.dirname(path), exist_ok=True)

        match file_type:
            case FileType.CSV:
                data.write_csv(path, **kwargs)
            case FileType.JSON:
                data.write_json(path, **kwargs)
            case FileType.PARQUET:
                data.write_parquet(path, **kwargs)
            case FileType.SQLITE3:
                data.write_database(
                    table_name=path,
                    connection=self.uri,
                    engine="sqlalchemy",
                    **kwargs,
                )
            case FileType.YAML:
                with open(path, "w", encoding="utf-8") as f:
                    yaml.safe_dump(data, f, sort_keys=False, allow_unicode=True)
            case _:
                raise NotImplementedError(f"`{file_type}` is not implemented")
        return True

    def extract_data(self, api_config: ApiConfig) -> Result[dict[str, Any], Exception]:
        call = f"http://api.weatherapi.com/v1/{api_config.request_type}.json?key={api_config.api_key}&q={api_config.location}&aqi=no"
        response = requests.get(call, timeout=10)

        if response.status_code == 200:  # noqa: PLR2004
            return Success(response.json())
        return Failure(response)
