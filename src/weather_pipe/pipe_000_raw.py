import argparse

from weather_pipe import events
from weather_pipe.handlers import EVENT_HANDLERS
from weather_pipe.io import IOWrapper
from weather_pipe.logger import FakeLogger
from weather_pipe.message_bus import MessageBus
from weather_pipe.uow import UnitOfWork

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse config path and API key.")
    parser.add_argument(
        "--config-path", type=str, help="Path to the configuration file"
    )
    parser.add_argument("--api-key", type=str, help="API key for authentication")
    args = vars(parser.parse_args())

    uow = UnitOfWork(repo=IOWrapper(), logger=FakeLogger())
    bus = MessageBus(event_handlers=EVENT_HANDLERS, uow=uow)
    bus.add_events([events.parse_event(args)])
    bus.handle_events()
