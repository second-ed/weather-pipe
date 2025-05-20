import sqlite3
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
    def setup(self) -> bool: ...

    def teardown(self) -> bool: ...

    def read(self, path: str, file_type: iof.FileType, **kwargs) -> Result[Data, Exception]: ...

    def write(
        self, data, path: str, file_type: iof.FileType, **kwargs
    ) -> Result[bool, Exception]: ...


@attrs.define
class LocalIOWrapper:
    db_conns: dict = attrs.field(default=attrs.Factory(dict))

    def setup(self) -> bool:
        return True

    def teardown(self) -> bool:
        return True

    def read(self, path: str, file_type: iof.FileType, **kwargs) -> Result[Data, Exception]:
        return iof.IO_READERS[file_type](path, **kwargs)

    def write(
        self, data: dict | pl.DataFrame, path: str, file_type: iof.FileType, **kwargs
    ) -> Result[bool, Exception]:
        return iof.IO_WRITERS[file_type](data, path, **kwargs)

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

    def setup(self) -> bool:
        return True

    def teardown(self) -> bool:
        return True

    def read(self, path: str, file_type: iof.FileType, **kwargs) -> Result[Data, Exception]:
        self.log.append({"func": "read", "path": path, "file_type": file_type, "kwargs": kwargs})
        try:
            return Success(self.db[file_type][path])
        except KeyError as e:
            return Failure({"err": str(e), "path": path})

    def write(
        self, data: dict | pl.DataFrame, path: str, file_type: iof.FileType, **kwargs
    ) -> Result[bool, Exception]:
        self.log.append({"func": "write", "path": path, "file_type": file_type, "kwargs": kwargs})
        self.db[file_type][path] = data
        return Success(True)

    def extract_data(self, api_config: ApiConfig) -> Result[dict[str, Any], Exception]:
        return Success(self.external_src[api_config.location])


@attrs.define
class SQLiteIOWrapper:
    db_path: str = attrs.field(default="")
    db_conn: pl._typing.ConnectionOrCursor = attrs.field(default=None)

    def setup(self) -> bool:
        self.db_conn = sqlite3.connect(self.db_path)
        return True

    def teardown(self) -> bool:
        self.db_conn.close()
        return True

    def read(self, query: str, **kwargs):
        return iof.IO_READERS[iof.FileType.SQLITE](query, connection=self.db_conn, **kwargs)

    def write(self, data: pl.DataFrame, table_name: str, **kwargs) -> Result[bool, Exception]:
        return iof.IO_WRITERS[iof.FileType.SQLITE](
            data=data, table_name=table_name, connection=self.db_conn, **kwargs
        )


@attrs.define
class FakeSQLiteIOWrapper:
    db_path: str = attrs.field(default="")
    db_conn: pl._typing.ConnectionOrCursor = attrs.field(default=None)
    db: defaultdict = attrs.field(default=None)
    log: list = attrs.field(default=attrs.Factory(list))

    def __attrs_post_init__(self):
        self.db = self.db or defaultdict(dict)

    def setup(self) -> bool:
        self.db_conn = True
        return True

    def teardown(self) -> bool:
        self.db_conn = False
        return True

    def read(self, query: str, **kwargs):
        self.log.append({"func": "read", "query": query, "kwargs": kwargs})
        try:
            query_list = query.split()
            table_idx = query_list.index("FROM") + 1
            table_name = query_list[table_idx]
            query_list[table_idx] = "self"
            pl_query = " ".join(query_list)
            return Success(self.db[table_name].sql(pl_query))
        except KeyError as e:
            return Failure({"err": str(e), "query": pl_query})

    def write(self, data: pl.DataFrame, table_name: str, **kwargs) -> Result[bool, Exception]:
        self.log.append({"func": "write", "table_name": table_name, "kwargs": kwargs})
        self.db[table_name] = data
        return Success(True)
