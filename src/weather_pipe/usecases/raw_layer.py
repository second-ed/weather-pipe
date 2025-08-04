from functools import partial

import attrs

from weather_pipe.adapters.io_wrappers._io_protocol import FileType
from weather_pipe.domain.data_structures import ApiConfig
from weather_pipe.domain.result import Result
from weather_pipe.domain.transform import add_ingestion_columns, clean_str, convert_json_to_df
from weather_pipe.service_layer.uow import UnitOfWorkProtocol
from weather_pipe.usecases._event import Event


@attrs.define
class IngestToRawZone(Event):
    config_path: str = attrs.field(default="")
    repo_root: str = attrs.field(default="")
    api_key: str = attrs.field(default="", repr=False)


def raw_layer_handler(event: IngestToRawZone, uow: UnitOfWorkProtocol) -> Result:
    with uow:
        uow.logger.info({"guid": uow.guid, "event": event})
        config_res = uow.repo.io.read(event.config_path, FileType.YAML)
        if not config_res.is_ok():
            uow.logger.error({"guid": uow.guid, "event": event, "result": config_res})
            return config_res
        config = config_res.inner

        api_config = ApiConfig(
            api_key=event.api_key,
            location=config.get("api_config", {}).get("location"),
            request_type=config.get("api_config", {}).get("request_type"),
        )
        init_convert_json_to_df = partial(
            convert_json_to_df,
            table_path=config.get("table_path", []),
        )
        init_add_ingestion_columns = partial(
            add_ingestion_columns,
            batch_guid=uow.guid,
            date_time=uow.start_time,
        )
        cleaned_location = clean_str(api_config.location)
        save_dir = f"{event.repo_root}/{config.get('save_dir', '')}/{cleaned_location}"
        filename = f"{uow.start_time}_{api_config.request_type}_{cleaned_location}"
        init_write_parquet = partial(
            uow.repo.io.write,
            path=f"{save_dir}/{filename}.parquet",
            file_type=FileType.PARQUET,
        )

        result = (
            uow.repo.io.extract_data(api_config)
            .bind(init_convert_json_to_df)
            .bind(init_add_ingestion_columns)
            .bind(init_write_parquet)
        )
        uow.logger.info({"guid": uow.guid, "event": event, "result": result})

    return result
