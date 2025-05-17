from typing import Protocol, runtime_checkable

import attrs


@runtime_checkable
class LoggerProtocol(Protocol):
    def debug(self, msg: str) -> None: ...

    def info(self, msg: str) -> None: ...

    def error(self, msg: str) -> None: ...


@attrs.define
class FakeLogger(LoggerProtocol):
    log: list = attrs.field(default=attrs.Factory(list))

    def notset(self, msg: str) -> None:
        self.log.append(f"NOTSET: {msg}")

    def debug(self, msg: str) -> None:
        self.log.append(f"DEBUG: {msg}")

    def info(self, msg: str) -> None:
        self.log.append(f"INFO: {msg}")

    def warning(self, msg: str) -> None:
        self.log.append(f"WARNING: {msg}")

    def warn(self, msg: str) -> None:
        self.log.append(f"WARN: {msg}")

    def error(self, msg: str) -> None:
        self.log.append(f"ERROR: {msg}")

    def fatal(self, msg: str) -> None:
        self.log.append(f"FATAL: {msg}")

    def critical(self, msg: str) -> None:
        self.log.append(f"CRITICAL: {msg}")
