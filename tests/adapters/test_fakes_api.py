import inspect
from collections.abc import Callable

import pytest

import weather_pipe.adapters.io_wrappers._io_protocol as io_protocol
from weather_pipe.adapters.fs_wrappers import _fs_protocol as fs_protocol
from weather_pipe.adapters.fs_wrappers import local_fs_wrapper
from weather_pipe.adapters.io_wrappers import pl_io


class SanityCheck:
    def method_a(self, a: int, b: float) -> float:
        return a * b


class FakeSanityCheck:
    def method_a(self, a: int, b: float) -> float:
        return a * b

    def some_test_helper_method(self, *, c: bool) -> bool:
        return c


class FakeMissingMethod:
    pass


class FakeMismatchingSignature:
    def method_a(self, a: int) -> int:
        return a


@pytest.mark.parametrize(
    ("real", "fake"),
    [
        pytest.param(
            SanityCheck(),
            FakeSanityCheck(),
            id="ensure matching public methods pass",
        ),
        pytest.param(
            SanityCheck(),
            FakeMissingMethod(),
            id="ensure fails if fake missing method",
            marks=pytest.mark.xfail(
                reason="ensure fails if fake missing method",
                strict=True,
            ),
        ),
        pytest.param(
            SanityCheck(),
            FakeMismatchingSignature(),
            id="ensure fails if fake not matching signature",
            marks=pytest.mark.xfail(
                reason="ensure fails if fake not matching signature",
                strict=True,
            ),
        ),
        pytest.param(
            pl_io.PolarsIO(),
            io_protocol.FakeIOWrapper(),
            id="ensure polars wrapper matches fake",
        ),
        pytest.param(
            pl_io.PolarsIO,
            io_protocol.IOWrapperProtocol,
            id="ensure polars wrapper matches protocol",
        ),
        pytest.param(
            local_fs_wrapper.LocalFileSystem(),
            fs_protocol.FakeFileSystem(),
            id="ensure local file system wrapper matches fake",
        ),
        pytest.param(
            local_fs_wrapper.LocalFileSystem,
            fs_protocol.FileSystemProtocol,
            id="ensure local file system wrapper matches protocol",
        ),
    ],
)
def test_api_match(real: object, fake: object) -> None:
    def get_methods(obj: Callable) -> dict[str, inspect.Signature]:
        return {
            name: inspect.signature(fn)
            for name, fn in inspect.getmembers(obj, inspect.isroutine)
            if not (name.startswith("__") and name.endswith("__"))
        }

    real_methods = get_methods(real)
    fake_methods = get_methods(fake)

    # all methods in the real are in the fake
    assert set(real_methods) - set(fake_methods) == set()

    # all methods in the real have the same signature as those in the fake
    mismatches = [
        {"method": key, "real": method, "fake": fake_methods[key]}
        for key, method in real_methods.items()
        if fake_methods[key] != method
    ]
    assert mismatches == []
