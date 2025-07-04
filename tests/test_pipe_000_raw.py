from collections import defaultdict

import pytest

from weather_pipe.adapters import repo
from weather_pipe.adapters.clock import fake_clock_now
from weather_pipe.adapters.fs_wrappers._fs_protocol import FakeFileSystem
from weather_pipe.adapters.io_wrappers._io_protocol import FakeIOWrapper, FileType
from weather_pipe.adapters.logger import FakeLogger
from weather_pipe.service_layer.message_bus import MessageBus
from weather_pipe.service_layer.uow import UnitOfWork
from weather_pipe.usecases import EVENT_HANDLERS, events

from .conftest import JSON_RESPONSE


@pytest.mark.parametrize(
    ("args", "expected_result"),
    [
        pytest.param(
            {
                "config_path": "path/to/config.yaml",
                "api_key": "123_abc",
                "repo_root": "weather_pipe",
            },
            [
                "INFO: {'guid': '123-abc', 'start_time': '20250527_194000', 'msg': 'Initialising UOW'}",
                "INFO: {'guid': '123-abc', 'event': IngestToRawZone(priority_event=False, config_path='path/to/config.yaml', repo_root='weather_pipe')}",
                "INFO: {'guid': '123-abc', 'event': IngestToRawZone(priority_event=False, config_path='path/to/config.yaml', repo_root='weather_pipe'), 'result': <Success: True>}",
                "INFO: {'guid': '123-abc', 'end_time': '20250527_194000', 'msg': 'Completed UOW'}",
            ],
            id="ensure has success when given a valid config",
        ),
        pytest.param(
            {
                "config_path": "invalid_path.yaml",
                "api_key": "123_abc",
                "repo_root": "weather_pipe",
            },
            [
                "INFO: {'guid': '123-abc', 'start_time': '20250527_194000', 'msg': 'Initialising UOW'}",
                "INFO: {'guid': '123-abc', 'event': IngestToRawZone(priority_event=False, config_path='invalid_path.yaml', repo_root='weather_pipe')}",
                "ERROR: {'guid': '123-abc', 'event': IngestToRawZone(priority_event=False, config_path='invalid_path.yaml', repo_root='weather_pipe'), 'result': <Failure: 'invalid_path.yaml'>}",
                "INFO: {'guid': '123-abc', 'end_time': '20250527_194000', 'msg': 'Completed UOW'}",
            ],
            id="ensure reports failure reason if given invalid config",
        ),
    ],
)
def test_raw_pipe(args, expected_result):
    def fixed_guid() -> str:
        return "123-abc"

    external_src = {"Liverpool": JSON_RESPONSE}
    db = {
        FileType.YAML: {
            "path/to/config.yaml": {
                "api_config": {"location": "Liverpool", "request_type": "forecast"},
                "table_path": ["forecast", "forecastday", 0, "hour"],
                "save_dir": "data/raw",
            },
        },
    }
    db = defaultdict(dict, db)
    event = events.parse_event(args)
    logger = FakeLogger()

    raw_repo = repo.Repo(io=FakeIOWrapper(db=db, external_src=external_src), fs=FakeFileSystem())
    bronze_repo = repo.Repo(
        io=FakeIOWrapper(db=db, external_src=external_src, db_name="bronze_layer"),
        fs=FakeFileSystem(),
    )

    uows = {
        events.IngestToRawZone: UnitOfWork(
            repo=raw_repo,
            logger=logger,
            clock_func=fake_clock_now,
            guid_func=fixed_guid,
        ),
        events.PromoteToBronzeLayer: UnitOfWork(
            repo=bronze_repo,
            logger=logger,
            clock_func=fake_clock_now,
            guid_func=fixed_guid,
        ),
    }

    bus = MessageBus(event_handlers=EVENT_HANDLERS, uows=uows)
    bus.add_events([event])
    bus.handle_events()

    # the pipe should've logged the expected result
    assert bus.uows[events.IngestToRawZone].logger.log == expected_result

    if "Success" in expected_result:
        assert len(bus.uows[events.IngestToRawZone].repo.io.db[FileType.PARQUET]) > 0
