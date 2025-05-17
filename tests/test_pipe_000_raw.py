import pytest

from weather_pipe import events
from weather_pipe.handlers import EVENT_HANDLERS
from weather_pipe.io import FakeIOWrapper
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
        ),
        pytest.param(
            {
                "config_path": "invalid_path.yaml",
                "api_key": "123_abc",
                "repo_root": "weather_pipe",
            },
            "'result': <Failure: {'err': \"'invalid_path.yaml'\", 'path': 'invalid_path.yaml'}>}",
        ),
    ),
)
def test_raw_pipe(args, expected_result):
    external_src = {"Liverpool": JSON_RESPONSE}
    db = {
        "path/to/config.yaml": {
            "api_config": {"location": "Liverpool", "request_type": "forecast"},
            "table_path": ["forecast", "forecastday", 0, "hour"],
            "save_dir": "data/raw",
        }
    }
    event = events.parse_event(args)
    uow = UnitOfWork(
        repo=FakeIOWrapper(db=db, external_src=external_src), logger=FakeLogger()
    )
    bus = MessageBus(event_handlers=EVENT_HANDLERS, uow=uow)
    bus.add_events([event])
    bus.handle_events()

    # make sure guids in all logs
    assert all("{'guid': " in log for log in bus.uow.logger.log)

    # the pipe should've logged the expected result
    assert any(expected_result in log for log in bus.uow.logger.log)
