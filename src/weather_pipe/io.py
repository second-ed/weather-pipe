import os
from typing import Any

import polars as pl
import requests
import yaml
from returns.result import Failure, Result, Success

from weather_pipe.data_structures import ApiConfig


def load_yaml(path: str) -> Result[dict, Exception]:
    try:
        with open(path, "r") as f:
            data = yaml.safe_load(f)

        if data is None:
            return Failure({"err": "empty yaml", "path": path})

        return Success(data)
    except Exception as e:
        return Failure({"err": str(e), "path": path})


def extract_data(api_config: ApiConfig) -> Result[dict[str, Any], Exception]:
    call = f"http://api.weatherapi.com/v1/{api_config.request_type}.json?key={api_config.api_key}&q={api_config.location}&aqi=no"
    response = requests.get(call, timeout=10)

    if response.status_code == 200:
        return Success(response.json())
    return Failure(response)


def save_parquet(df: pl.DataFrame, save_path: str) -> Result[bool, Exception]:
    try:
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        df.write_parquet(save_path)
        return Success(True)
    except Exception as e:
        return Failure({"err": str(e)})
