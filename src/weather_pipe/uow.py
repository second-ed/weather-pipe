import uuid
from datetime import datetime
from typing import Callable, Protocol, runtime_checkable

import attrs

from weather_pipe.io import IOWrapperProtocol
from weather_pipe.logger import LoggerProtocol


@runtime_checkable
class UnitOfWorkProtocol(Protocol):
    def __enter__(self): ...

    def __exit__(self, exc_type, exc_val, exc_tb): ...


@attrs.define
class UnitOfWork(UnitOfWorkProtocol):
    repo: IOWrapperProtocol = attrs.field()
    logger: LoggerProtocol = attrs.field()
    guid_generator: Callable = attrs.field(default=uuid.uuid4)
    guid: str = attrs.field(default="")
    start_time: str = attrs.field(default="")

    def __enter__(self):
        self.guid = str(self.guid_generator())
        self.start_time = datetime.now().strftime("%y%m%d_%H%M%S")
        self.logger.info({"guid": self.guid, "msg": "Initialising UOW"})
        self.repo.setup()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.logger.error({"guid": self.guid, "msg": exc_val})
        else:
            self.logger.info({"guid": self.guid, "msg": "Completed UOW"})

        self.repo.teardown()
