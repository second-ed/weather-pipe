from functools import partial

from returns.pipeline import is_successful, pipe
from returns.pointfree import bind
from returns.result import Failure

from weather_pipe.adapters.io_wrappers._io_protocol import FileType
from weather_pipe.domain.data_structures import ApiConfig
from weather_pipe.domain.transform import add_ingestion_columns, clean_str, convert_json_to_df
from weather_pipe.service_layer.uow import UnitOfWorkProtocol
from weather_pipe.usecases.events import IngestToRawZone, PromoteToBronzeLayer


def raw_layer_handler(event: IngestToRawZone, uow: UnitOfWorkProtocol) -> None:
    with uow:
        uow.logger.info({"guid": uow.guid, "event": event})
        config_res = uow.repo.io.read(event.config_path, FileType.YAML)
        if isinstance(config_res, Failure):
            uow.logger.error({"guid": uow.guid, "event": event, "result": config_res})
            return Failure({"err": config_res.failure()})
        config = config_res.unwrap()

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

        pipeline = pipe(
            uow.repo.io.extract_data,
            bind(init_convert_json_to_df),
            bind(init_add_ingestion_columns),
            bind(init_write_parquet),
        )
        result = pipeline(api_config)
        uow.logger.info({"guid": uow.guid, "event": event, "result": result})

    if is_successful(result):
        return PromoteToBronzeLayer()
    return None


def bronze_layer_handler(event: PromoteToBronzeLayer, uow: UnitOfWorkProtocol) -> None:
    pass
