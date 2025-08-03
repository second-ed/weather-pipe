from __future__ import annotations

from typing import TYPE_CHECKING

import attrs

if TYPE_CHECKING:
    import polars as pl


@attrs.define
class ApiConfig:
    api_key: str = attrs.field(repr=False)
    location: str = attrs.field()
    request_type: str = attrs.field()


@attrs.define
class RawTable:
    table: pl.DataFrame = attrs.field(repr=False)


@attrs.define
class UnnestedTable(RawTable):
    pass


@attrs.define
class CleanedTable(RawTable):
    pass


@attrs.define
class EncodedTable(RawTable):
    pass
