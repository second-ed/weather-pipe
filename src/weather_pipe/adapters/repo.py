from typing import Protocol, runtime_checkable

import attrs
from attrs.validators import instance_of, optional

from weather_pipe.adapters.fs_wrappers._fs_protocol import (
    FakeFileSystem,
    FileSystemProtocol,
)
from weather_pipe.adapters.io_wrappers._io_protocol import FakeIOWrapper, IOWrapperProtocol


@runtime_checkable
class RepoProtocol(Protocol):
    fs: FileSystemProtocol
    io: IOWrapperProtocol


@attrs.define
class Repo:
    fs: FileSystemProtocol = attrs.field(
        default=None,
        validator=optional(instance_of(FileSystemProtocol)),
    )
    io: IOWrapperProtocol = attrs.field(
        default=None,
        validator=optional(instance_of(IOWrapperProtocol)),
    )


@attrs.define
class FakeRepo:
    db: dict = attrs.field(default=attrs.Factory(dict))
    fs: FileSystemProtocol = attrs.field(
        default=None,
        validator=optional(instance_of(FileSystemProtocol)),
    )
    io: IOWrapperProtocol = attrs.field(
        default=None,
        validator=optional(instance_of(IOWrapperProtocol)),
    )

    def __attrs_post_init__(self) -> None:
        self.fs = FakeFileSystem(self.db)
        self.io = FakeIOWrapper(self.db)
