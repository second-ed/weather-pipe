import argparse
import uuid
from datetime import datetime
from functools import partial
from pathlib import Path

from returns.pipeline import pipe
from returns.pointfree import bind
from returns.result import Failure, Result

from weather_pipe import io
from weather_pipe.data_structures import ApiConfig
from weather_pipe.transform import add_ingestion_columns, convert_json_to_df
from weather_pipe.utils import clean_str

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
    cleaned_location = clean_str(api_config.location)
    filename = f"{date_time.strftime('%y%m%d_%H%M%S')}_{api_config.request_type}_{cleaned_location}"

    init_convert_json_to_df = partial(
        convert_json_to_df, table_path=config.get("table_path", [])
    )
    init_add_ingestion_columns = partial(
        add_ingestion_columns, batch_guid=batch_guid, date_time=date_time
    )
    init_save_parquet = partial(
        io.save_parquet,
        save_path=f"{REPO_ROOT.joinpath(config.get('save_dir'), cleaned_location)}/{filename}.parquet",
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
    parser.add_argument(
        "--config-path", type=str, help="Path to the configuration file"
    )
    parser.add_argument("--api-key", type=str, help="API key for authentication")
    args = parser.parse_args()

    print(run_raw_layer(args.config_path, args.api_key))
