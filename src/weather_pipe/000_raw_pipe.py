import argparse
import uuid
from datetime import datetime
from functools import partial
from pathlib import Path

from returns.pipeline import pipe
from returns.pointfree import bind
from returns.result import Failure, Result

from . import io
from .data_structures import ApiConfig
from .transform import add_ingestion_columns, convert_json_to_df

REPO_ROOT = Path(__file__).parents[2]


def run_raw_layer(config_path: str, api_key: str) -> Result[bool, Exception]:
    config_res = io.load_yaml(config_path)
    if isinstance(config_res, Failure):
        return Failure({"err": config_res.failure()})
    config = config_res.unwrap()

    batch_guid = str(uuid.uuid4())
    date_time = datetime.now()
    api_config = ApiConfig(
        api_key=api_key,
        location=config.get("api_config", {}).get("location"),
        request_type=config.get("api_config", {}).get("request_type"),
    )
    filename = f"{date_time.strftime('%y%m%d_%H%M%S')}_{api_config.request_type}_{api_config.location}"

    init_convert_json_to_df = partial(
        convert_json_to_df, table_path=config.get("table_path", [])
    )
    init_add_ingestion_columns = partial(
        add_ingestion_columns, batch_guid=batch_guid, date_time=date_time
    )
    init_save_parquet = partial(
        io.save_parquet,
        save_path=f"{REPO_ROOT.joinpath(config.get('save_dir'), api_config.location)}/{filename}.parquet",
    )

    pipeline = pipe(
        io.extract_data,
        bind(init_convert_json_to_df),
        bind(init_add_ingestion_columns),
        bind(init_save_parquet),
    )

    return pipeline(api_config)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse config path and API key.")
    parser.add_argument("config_path", type=str, help="Path to the configuration file")
    parser.add_argument("api_key", type=str, help="API key for authentication")
    args = parser.parse_args()

    run_raw_layer(args.config_path, args.api_key)
