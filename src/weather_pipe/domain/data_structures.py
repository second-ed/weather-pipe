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
class RawTables:
    fact_table: pl.DataFrame = attrs.field(repr=False)
    dim_tables: dict | None = attrs.field(default=None, repr=False)
    cols: list | None = attrs.field(default=None)


@attrs.define
class UnnestedTables(RawTables):
    pass


@attrs.define
class CleanedTables(RawTables):
    pass


@attrs.define
class NormalisedTables(RawTables):
    pass


@attrs.define
class EncodedTables(RawTables):
    pass
