from __future__ import annotations

from typing import TYPE_CHECKING

import attrs

from weather_pipe.v2.adapters.io_funcs import FileType
from weather_pipe.v2.core.event import Event, register_event_handler
from weather_pipe.v2.core.logger import logger
from weather_pipe.v2.layers.raw.data_structures import ApiConfig
from weather_pipe.v2.layers.raw.transform import (
    add_ingestion_columns,
    convert_json_to_df,
    generate_raw_save_path,
)

if TYPE_CHECKING:
    from danom import Result
    from io_adapters import IoAdapter


@attrs.define(frozen=True)
class IngestToRawZone(Event):
    config_path: str = attrs.field()
    repo_root: str = attrs.field()
    api_key: str = attrs.field(repr=False)


@register_event_handler(IngestToRawZone)
def run_raw_layer(event: IngestToRawZone, adapter: IoAdapter) -> Result:
    config_res = adapter.read(event.config_path, FileType.YAML)
    logger.info(f"{config_res = }")

    if not config_res.is_ok():
        return config_res

    config = config_res.unwrap()

    api_config = ApiConfig.from_config(config, event.api_key)
    start_time = adapter.get_datetime()
    raw_save_path = generate_raw_save_path(
        api_config,
        event.repo_root,
        config.get("save_dir", ""),
        start_time,
    )

    return (
        adapter.read(api_config.to_str(), FileType.API_CALL)
        .and_then(convert_json_to_df, table_path=config.get("table_path", []))
        .and_then(
            add_ingestion_columns,
            location=api_config.location,
            batch_guid=adapter.get_guid(),
            date_time=start_time,
        )
        .and_then(
            adapter.write,
            path=raw_save_path,
            file_type=FileType.PARQUET,
        )
    )
