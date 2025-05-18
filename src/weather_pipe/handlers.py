from functools import partial
from typing import Callable, Sequence

from returns.pipeline import is_successful, pipe
from returns.pointfree import bind
from returns.result import Failure

from weather_pipe.data_structures import ApiConfig
from weather_pipe.events import Event, IngestToRawZone, PromoteToBronzeLayer
from weather_pipe.io_funcs import FileType
from weather_pipe.transform import add_ingestion_columns, clean_str, convert_json_to_df
from weather_pipe.uow import UnitOfWorkProtocol

EventHandlers = dict[type, Callable[[Event], Event | Sequence[Event] | None]]


def raw_layer_handler(event: IngestToRawZone, uow: UnitOfWorkProtocol):
    with uow:
        uow.logger.info({"guid": uow.guid, "event": event})
        config_res = uow.repo.read(event.config_path, FileType.YAML)
        if isinstance(config_res, Failure):
            uow.logger.error({"guid": uow.guid, "result": config_res})
            return Failure({"err": config_res.failure()})
        config = config_res.unwrap()

        api_config = ApiConfig(
            api_key=event.api_key,
            location=config.get("api_config", {}).get("location"),
            request_type=config.get("api_config", {}).get("request_type"),
        )
        init_convert_json_to_df = partial(
            convert_json_to_df, table_path=config.get("table_path", [])
        )
        init_add_ingestion_columns = partial(
            add_ingestion_columns, batch_guid=uow.guid, date_time=uow.start_time
        )

        cleaned_location = clean_str(api_config.location)
        filename = f"{uow.start_time}_{api_config.request_type}_{cleaned_location}"
        init_write_parquet = partial(
            uow.repo.write,
            path=f"{event.repo_root}/{config.get('save_dir', '')}/{cleaned_location}/{filename}.parquet",
            file_type=FileType.PARQUET,
        )

        pipeline = pipe(
            uow.repo.extract_data,
            bind(init_convert_json_to_df),
            bind(init_add_ingestion_columns),
            bind(init_write_parquet),
        )
        result = pipeline(api_config)
        uow.logger.info({"guid": uow.guid, "result": result})

    if is_successful(result):
        return PromoteToBronzeLayer()
    return None


def bronze_layer_handler(event: PromoteToBronzeLayer, uow: UnitOfWorkProtocol):
    pass


EVENT_HANDLERS = {
    IngestToRawZone: raw_layer_handler,
    PromoteToBronzeLayer: bronze_layer_handler,
}
