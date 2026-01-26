from copy import deepcopy
from pathlib import Path

import duckdb
import polars as pl
import yaml

from weather_pipe.v2.core.constants import REPO_ROOT

TESTS = {
    "fact_id": ["unique", "not_null"],
    "sys_col_row_guid": ["unique", "not_null"],
    "wind_dir_id": [
        "not_null",
        {"relationships": {"arguments": {"to": "ref('dim_wind_dir')", "field": "id"}}},
    ],
    "id": ["unique", "not_null"],
    "location_id": [
        "not_null",
        {"relationships": {"arguments": {"to": "ref('dim_location')", "field": "id"}}},
    ],
    "condition_id": [
        "not_null",
        {"relationships": {"arguments": {"to": "ref('dim_condition')", "field": "id"}}},
    ],
    "location": [{"accepted_values": {"arguments": {"values": ["liverpool", "Liverpool"]}}}],
    "month": [
        {
            "dbt_utils.accepted_range": {
                "arguments": {"min_value": 1, "max_value": 12, "inclusive": True},
            },
        },
    ],
    "hour": [
        {
            "dbt_utils.accepted_range": {
                "arguments": {"min_value": 0, "max_value": 23, "inclusive": True},
            },
        },
    ],
    "is_day": [
        {
            "accepted_values": {
                "arguments": {
                    "values": [
                        0,
                        1,
                    ],
                    "quote": False,
                },
            },
        },
    ],
    "sys_col_batch_guid": ["not_null"],
    "sys_col_filename": ["not_null"],
    "sys_col_ingestion_datetime": ["not_null"],
    "chance_of_rain": [
        {
            "dbt_utils.accepted_range": {
                "arguments": {"min_value": 0, "max_value": 100, "inclusive": True},
            },
        },
    ],
    "chance_of_snow": [
        {
            "dbt_utils.accepted_range": {
                "arguments": {"min_value": 0, "max_value": 100, "inclusive": True},
            },
        },
    ],
    "temp_c": [
        {
            "dbt_utils.accepted_range": {
                "arguments": {"min_value": -20, "max_value": 60, "inclusive": True},
            },
        },
    ],
    "will_it_rain": [
        {
            "dbt_utils.accepted_range": {
                "arguments": {"min_value": 0, "max_value": 1, "inclusive": True},
            },
        },
    ],
    "will_it_snow": [
        {
            "dbt_utils.accepted_range": {
                "arguments": {"min_value": 0, "max_value": 1, "inclusive": True},
            },
        },
    ],
    "precip_mm": [
        {
            "dbt_utils.accepted_range": {
                "arguments": {"min_value": 0, "max_value": 120, "inclusive": True},
            },
        },
    ],
    "gust_mph": [
        {
            "dbt_utils.accepted_range": {
                "arguments": {"min_value": 0, "max_value": 120, "inclusive": True},
            },
        },
    ],
    "wind_kph": [
        {
            "dbt_utils.accepted_range": {
                "arguments": {"min_value": 0, "max_value": 120, "inclusive": True},
            },
        },
    ],
    "gust_kph": [
        {
            "dbt_utils.accepted_range": {
                "arguments": {"min_value": 0, "max_value": 120, "inclusive": True},
            },
        },
    ],
    "precip_in": [
        {
            "dbt_utils.accepted_range": {
                "arguments": {"min_value": 0, "max_value": 50, "inclusive": True},
            },
        },
    ],
    "snow_cm": [
        {
            "dbt_utils.accepted_range": {
                "arguments": {"min_value": 0, "max_value": 120, "inclusive": True},
            },
        },
    ],
    "wind_mph": [
        {
            "dbt_utils.accepted_range": {
                "arguments": {"min_value": 0, "max_value": 120, "inclusive": True},
            },
        },
    ],
}


def get_column(name: str) -> dict:
    return {"name": name, "tests": deepcopy(TESTS.get(name, []))}


def get_model(table: str, df: pl.DataFrame) -> dict:
    return {"name": table, "columns": [get_column(col) for col in df.columns]}


def get_schema(tables: list[str]) -> dict:
    models = []

    for table in tables:
        con = duckdb.connect("dev.duckdb")
        df = con.execute(f"""select * from {table}""").pl()  # noqa: S608 PD901
        models.append(get_model(table, df))
    con.close()
    return {"version": 2, "models": models}


if __name__ == "__main__":
    layers = {
        "bronze": ["bronze", "raw_to_bronze"],
        "silver": ["dim_condition", "dim_location", "dim_wind_dir", "fact_weather"],
        "gold": ["location_hourly_averages", "location_monthly_averages"],
    }

    for k, v in layers.items():
        schema = get_schema(v)
        Path(
            f"{REPO_ROOT}/warehouse/models/{k}/schema.yaml",
        ).write_text(
            yaml.dump(schema, Dumper=yaml.SafeDumper, sort_keys=False, default_flow_style=False),
        )
