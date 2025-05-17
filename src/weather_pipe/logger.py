from typing import Any, Protocol, runtime_checkable

import attrs
import structlog


@runtime_checkable
class LoggerProtocol(Protocol):
    def debug(self, msg: str, **kwargs: Any) -> None: ...

    def info(self, msg: str, **kwargs: Any) -> None: ...

    def error(self, msg: str, **kwargs: Any) -> None: ...


@attrs.define
class StructLogger:
    logger: structlog.BoundLogger = attrs.Factory(structlog.get_logger)

    def debug(self, msg: str, **kwargs: Any) -> None:
        self.logger.debug(msg, **kwargs)

    def info(self, msg: str, **kwargs: Any) -> None:
        self.logger.info(msg, **kwargs)

    def error(self, msg: str, **kwargs: Any) -> None:
        self.logger.error(msg, **kwargs)


@attrs.define
class FakeLogger(LoggerProtocol):
    log: list = attrs.field(default=attrs.Factory(list))

    def debug(self, msg: str, **kwargs: Any) -> None:
        self.log.append(f"DEBUG: {msg}")

    def info(self, msg: str, **kwargs: Any) -> None:
        self.log.append(f"INFO: {msg}")

    def error(self, msg: str, **kwargs: Any) -> None:
        self.log.append(f"ERROR: {msg}")
