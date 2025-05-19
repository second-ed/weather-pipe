import argparse
from pathlib import Path

import structlog

from weather_pipe import events
from weather_pipe.handlers import EVENT_HANDLERS
from weather_pipe.io import IOWrapper
from weather_pipe.logger import StructLogger
from weather_pipe.message_bus import MessageBus
from weather_pipe.uow import UnitOfWork

REPO_ROOT = Path(__file__).parents[2]

if __name__ == "__main__":
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            structlog.processors.JSONRenderer(indent=4),
        ],
        logger_factory=structlog.PrintLoggerFactory(),
    )

    parser = argparse.ArgumentParser(description="Parse config path and API key.")
    parser.add_argument(
        "--config-path", type=str, help="Path to the configuration file"
    )
    parser.add_argument("--api-key", type=str, help="API key for authentication")
    args = vars(parser.parse_args())
    args["repo_root"] = str(REPO_ROOT)
    logger = StructLogger()
    uow = UnitOfWork(repo=IOWrapper(), logger=logger)
    bus = MessageBus(event_handlers=EVENT_HANDLERS, uow=uow)
    bus.add_events([events.parse_event(args)])
    bus.handle_events()
