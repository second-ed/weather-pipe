import pytest
from danom import safe
from io_adapters import get_fake_adapter

from tests.conftest import JSON_RESPONSE, RAW_PARQUET_RESULT
from weather_pipe.v2.adapters.io_funcs import Zone
from weather_pipe.v2.core.event import EVENT_HANDLERS
from weather_pipe.v2.core.message_bus import MessageBus
from weather_pipe.v2.layers.raw.pipe import IngestToRawZone


@pytest.mark.parametrize(
    ("args", "expected_result"),
    [
        pytest.param(
            {
                "config_path": "path/to/config.yaml",
                "api_key": "123_abc",
                "repo_root": "weather_pipe",
            },
            RAW_PARQUET_RESULT,
            id="ensure has success when given a valid config",
        ),
    ],
)
def test_raw_pipe(args, expected_result):
    args = {
        "config_path": "path/to/config.yaml",
        "api_key": "123_abc",
        "repo_root": "weather_pipe",
    }

    files = {
        "http://api.weatherapi.com/v1/forecast.json?key=123_abc&q=Liverpool&aqi=no": JSON_RESPONSE,
        "path/to/config.yaml": {
            "api_config": {"location": "Liverpool", "request_type": "forecast"},
            "table_path": ["forecast", "forecastday", 0, "hour"],
            "save_dir": "data/raw",
        },
    }

    adapter = get_fake_adapter(Zone.RAW, files)
    adapter.read_fns = {k: safe(v) for k, v in adapter.read_fns.items()}
    adapter.write_fns = {k: safe(v) for k, v in adapter.write_fns.items()}

    bus = (
        MessageBus(event_handlers=EVENT_HANDLERS, adapter=adapter)
        .add_events([IngestToRawZone.from_dict(args)])
        .handle_events()
    )

    bus.adapter.files[
        "weather_pipe/data/raw/liverpool/20250101_120000_forecast_liverpool.parquet"
    ].drop("row_guid").to_dicts()
