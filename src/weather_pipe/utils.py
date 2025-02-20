import functools
import os
import re
from logging.config import dictConfig
from pathlib import Path
from typing import Callable

import structlog

REPO_ROOT = Path(__file__).parents[2]
os.makedirs(REPO_ROOT.joinpath("data/logs"), exist_ok=True)

dictConfig(
    {
        "version": 1,
        "disable_existing_loggers": False,
        "handlers": {
            "file": {
                "class": "logging.handlers.TimedRotatingFileHandler",
                "filename": REPO_ROOT.joinpath("data/logs/current.log"),
                "when": "midnight",
                "interval": 1,
                "backupCount": 7,
                "encoding": "utf-8",
                "formatter": "simple",
            }
        },
        "formatters": {"simple": {"format": "%(message)s,"}},
        "root": {"level": "INFO", "handlers": ["file"]},
    }
)

structlog.configure_once(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer(indent=4),
    ],
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

log = structlog.get_logger()


def log_call(func: Callable) -> Callable:
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        log.info("pre-call", func=func.__name__, args=args, kwargs=kwargs)
        result = func(*args, **kwargs)
        log.info("post-result", func=func.__name__, result=result)
        return result

    return wrapper


def clean_str(inp_str: str) -> str:
    return re.sub(r"_+", "_", re.sub(r"[^a-zA-Z0-9]", "_", inp_str)).lower()
