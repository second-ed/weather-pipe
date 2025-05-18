import inspect

import pytest

from weather_pipe.io import FakeIOWrapper, IOWrapper
from weather_pipe.logger import FakeLogger, StructLogger


class SanityCheck:
    def method_a(self, a: int, b: float) -> float:
        return a * b


class FakeSanityCheck:
    def method_a(self, a: int, b: float) -> float:
        return a * b

    def some_test_helper_method(self, c: bool) -> bool:
        return c


class FakeMissingMethod:
    pass


class FakeMismatchingSignature:
    def method_a(self, a: int):
        return a


@pytest.mark.parametrize(
    "real, fake",
    (
        pytest.param(
            SanityCheck(), FakeSanityCheck(), id="ensure matching public methods pass"
        ),
        pytest.param(
            SanityCheck(),
            FakeMissingMethod(),
            id="ensure fails if fake missing method",
            marks=pytest.mark.xfail(
                reason="ensure fails if fake missing method", strict=True
            ),
        ),
        pytest.param(
            SanityCheck(),
            FakeMismatchingSignature(),
            id="ensure fails if fake not matching signature",
            marks=pytest.mark.xfail(
                reason="ensure fails if fake not matching signature", strict=True
            ),
        ),
        pytest.param(
            StructLogger(),
            FakeLogger(),
            id="ensure logger matching public methods pass",
        ),
        pytest.param(
            IOWrapper(),
            FakeIOWrapper(),
            id="ensure IO wrappers matching public methods pass",
        ),
    ),
)
def test_api_match(real, fake):
    def get_methods(obj):
        return dict(
            [
                (name, inspect.signature(fn))
                for name, fn in inspect.getmembers(obj, inspect.isroutine)
                if not name.startswith("_")  # only care about the public api
            ]
        )

    real_methods = get_methods(real)
    fake_methods = get_methods(fake)

    # all methods in the real are in the fake
    assert set(real_methods) - set(fake_methods) == set()

    # all methods in the real have the same signature as those in the fake
    assert set(real_methods.values()) - set(fake_methods.values()) == set()
