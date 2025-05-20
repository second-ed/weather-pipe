from collections import defaultdict

import pytest

import weather_pipe.io as io_mod
from weather_pipe import events
from weather_pipe.handlers import EVENT_HANDLERS
from weather_pipe.io_funcs import FileType
from weather_pipe.logger import FakeLogger
from weather_pipe.message_bus import MessageBus
from weather_pipe.uow import UnitOfWork

from .conftest import JSON_RESPONSE


@pytest.mark.parametrize(
    "args, expected_result",
    (
        pytest.param(
            {
                "config_path": "path/to/config.yaml",
                "api_key": "123_abc",
                "repo_root": "weather_pipe",
            },
            "'result': <Success: True>",
            id="ensure has success when given a valid config",
        ),
        pytest.param(
            {
                "config_path": "invalid_path.yaml",
                "api_key": "123_abc",
                "repo_root": "weather_pipe",
            },
            "'result': <Failure: {'err': \"'invalid_path.yaml'\", 'path': 'invalid_path.yaml'}>}",
            id="ensure reports failure reason if given invalid config",
        ),
    ),
)
def test_raw_pipe(args, expected_result):
    external_src = {"Liverpool": JSON_RESPONSE}
    db = db = {
        FileType.YAML: {
            "path/to/config.yaml": {
                "api_config": {"location": "Liverpool", "request_type": "forecast"},
                "table_path": ["forecast", "forecastday", 0, "hour"],
                "save_dir": "data/raw",
            }
        }
    }
    db = defaultdict(dict, db)
    event = events.parse_event(args)
    logger = FakeLogger()
    uows = {
        events.IngestToRawZone: UnitOfWork(
            repo=io_mod.FakeLocalIOWrapper(db=db, external_src=external_src), logger=logger
        ),
        events.PromoteToBronzeLayer: UnitOfWork(
            repo=io_mod.FakeSQLiteIOWrapper(db_path="fake/path/to/db"),
            logger=logger,
        ),
    }
    bus = MessageBus(event_handlers=EVENT_HANDLERS, uows=uows)
    bus.add_events([event])
    bus.handle_events()

    # make sure guids in all logs
    assert all("{'guid': " in log for log in bus.uows[events.IngestToRawZone].logger.log)

    # the pipe should've logged the expected result
    assert any(expected_result in log for log in bus.uows[events.IngestToRawZone].logger.log)

    if "Success" in expected_result:
        assert len(bus.uows[events.IngestToRawZone].repo.db[FileType.PARQUET]) > 0
