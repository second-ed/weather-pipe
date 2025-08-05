from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Protocol, runtime_checkable

import attrs

from weather_pipe.adapters.clock import clock_now

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType

    from weather_pipe.adapters.logger import LoggerProtocol
    from weather_pipe.adapters.repo import RepoProtocol


@runtime_checkable
class UnitOfWorkProtocol(Protocol):
    def __enter__(self) -> None: ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        _: TracebackType | None,
    ) -> None: ...


@attrs.define
class UnitOfWork:
    repo: RepoProtocol = attrs.field()
    logger: LoggerProtocol = attrs.field()
    clock_func: Callable[[str], str] = attrs.field(default=clock_now)
    guid_func: Callable[[], str] = attrs.field(default=uuid.uuid4)
    guid: str = attrs.field(default="")
    start_time: str = attrs.field(default="")

    def __enter__(self) -> None:
        self.guid = str(self.guid_func())
        self.start_time = self.clock_func()
        self.logger.info(
            {"guid": self.guid, "start_time": str(self.start_time), "msg": "Initialising UOW"},
        )

        # generic setup ops
        self.repo.io.setup()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        _: TracebackType | None,
    ) -> None:
        if exc_type is not None:
            self.logger.error(
                {
                    "guid": self.guid,
                    "end_time": str(self.clock_func()),
                    "msg": exc_val,
                },
            )
        else:
            self.logger.info(
                {"guid": self.guid, "end_time": str(self.clock_func()), "msg": "Completed UOW"},
            )

        # clean up afterwards
        self.repo.io.teardown()
