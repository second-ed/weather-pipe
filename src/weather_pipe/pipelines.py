import uuid
from datetime import datetime
from functools import partial

from returns.pipeline import pipe
from returns.pointfree import bind
from returns.result import Failure, Result

from . import io
from .data_structures import ApiConfig
from .transform import add_ingestion_columns, get_table_from_json


def run_raw_layer(config_path: str) -> Result[bool, Exception]:

    config_res = io.load_yaml(config_path)
    if isinstance(config_res, Failure):
        return Failure({"err": config_res.failure()})
    config = config_res.unwrap()

    batch_guid = str(uuid.uuid4())
    date_time = datetime.now()
    api_config = ApiConfig(**config["api_config"])
    filename = f"{date_time.strftime('%y%m%d_%H%M%S')}_{api_config.request_type}_{api_config.location}"

    init_get_table_from_json = partial(get_table_from_json, table_path=config.get("table_path", []))
    init_add_ingestion_columns = partial(add_ingestion_columns, batch_guid=batch_guid, date_time=date_time)
    init_save_parquet = partial(
        io.save_parquet,
        save_path=f"{config.get('save_dir')}/{api_config.location}/{filename}.parquet"
    )

    pipeline = pipe(
        io.extract_data,
        bind(init_get_table_from_json),
        bind(init_add_ingestion_columns),
        bind(init_save_parquet)
    )

    return pipeline(api_config)

