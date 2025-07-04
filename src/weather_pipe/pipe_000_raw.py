import argparse
from pathlib import Path

import structlog

from weather_pipe.adapters import repo
from weather_pipe.adapters.fs_wrappers.local_fs_wrapper import LocalFileSystem
from weather_pipe.adapters.io_wrappers.pl_io import PolarsIO
from weather_pipe.adapters.logger import StructLogger
from weather_pipe.service_layer.message_bus import MessageBus
from weather_pipe.service_layer.uow import UnitOfWork
from weather_pipe.usecases import EVENT_HANDLERS, events

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
    parser.add_argument("--config-path", type=str, help="Path to the configuration file")
    parser.add_argument("--api-key", type=str, help="API key for authentication")
    args = vars(parser.parse_args())

    args["repo_root"] = str(REPO_ROOT)

    logger = StructLogger()
    raw_repo = repo.Repo(io=PolarsIO(), fs=LocalFileSystem())
    bronze_repo = repo.Repo(
        io=PolarsIO(db_name=REPO_ROOT.joinpath("data", "bronze", "database.db")),
        fs=LocalFileSystem(),
    )
    uows = {
        events.IngestToRawZone: UnitOfWork(repo=raw_repo, logger=logger),
        events.PromoteToBronzeLayer: UnitOfWork(
            repo=bronze_repo,
            logger=logger,
        ),
    }

    bus = MessageBus(event_handlers=EVENT_HANDLERS, uows=uows)
    bus.add_events([events.parse_event(args)])
    bus.handle_events()
