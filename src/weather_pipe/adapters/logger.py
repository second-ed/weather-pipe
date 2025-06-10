from typing import Protocol, runtime_checkable

import attrs
import structlog


@runtime_checkable
class LoggerProtocol(Protocol):
    def debug(self, msg: str, **kwargs: dict) -> None: ...

    def info(self, msg: str, **kwargs: dict) -> None: ...

    def error(self, msg: str, **kwargs: dict) -> None: ...


@attrs.define
class StructLogger:
    logger: structlog.BoundLogger = attrs.Factory(structlog.get_logger)

    def debug(self, msg: str, **kwargs: dict) -> None:
        self.logger.debug(msg, **kwargs)

    def info(self, msg: str, **kwargs: dict) -> None:
        self.logger.info(msg, **kwargs)

    def error(self, msg: str, **kwargs: dict) -> None:
        self.logger.error(msg, **kwargs)


@attrs.define
class FakeLogger(LoggerProtocol):
    log: list = attrs.field(default=attrs.Factory(list))

    def debug(self, msg: str, **kwargs: dict) -> None:
        self.log.append(f"DEBUG: {msg}", **kwargs)

    def info(self, msg: str, **kwargs: dict) -> None:
        self.log.append(f"INFO: {msg}", **kwargs)

    def error(self, msg: str, **kwargs: dict) -> None:
        self.log.append(f"ERROR: {msg}", **kwargs)
