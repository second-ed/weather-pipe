from __future__ import annotations

from enum import Enum, unique
from pathlib import Path
from typing import TYPE_CHECKING

import requests
import yaml
from danom import safe
from io_adapters import (
    add_domain,
    register_domain_read_fn,
    register_domain_write_fn,
)

if TYPE_CHECKING:
    import polars as pl


@unique
class Zone(Enum):
    RAW = "raw"


@unique
class FileType(Enum):
    YAML = "yaml"
    PARQUET = "parquet"
    API_CALL = "api_call"


add_domain(Zone.RAW)


@register_domain_read_fn(Zone.RAW, FileType.API_CALL)
@safe
def extract_data(url: str, **kwargs: dict) -> dict:
    response = requests.get(url, timeout=10, **kwargs)
    response.raise_for_status()
    return response.json()


@register_domain_read_fn(Zone.RAW, FileType.YAML)
@safe
def read_yaml(path: str | Path, **kwargs: dict) -> dict:
    return yaml.safe_load(Path(path).read_text(**kwargs))


@register_domain_write_fn(Zone.RAW, FileType.PARQUET)
@safe
def write_parquet(df: pl.DataFrame, path: str | Path, **kwargs: dict) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path, **kwargs)
