from __future__ import annotations

import functools
from typing import TYPE_CHECKING, Any, Callable, Literal, Self, TypeVar

import attrs

if TYPE_CHECKING:
    from types import TracebackType

T = TypeVar("T")
U = TypeVar("U")


@attrs.define
class Ok:
    inner = attrs.field(
        default=None,
    )

    def is_ok(self) -> Literal[True]:
        return True

    def map(self, func: Callable[[T], U]) -> Ok[U]:
        return Ok(func(self.inner))

    def flatten(self) -> Ok[U]:
        inner = self.inner
        while isinstance(inner, Ok):
            inner = inner.inner
        return Ok(inner)

    def bind(self, func: Callable[[T], U]) -> Result:
        return func(self.inner)


@attrs.define
class Err:
    inner = attrs.field(repr=False)
    error: Exception = attrs.field(
        default=None,
        validator=attrs.validators.optional(attrs.validators.instance_of(Exception)),
    )
    err_type: BaseException = attrs.field(init=False, repr=False)
    err_msg: str = attrs.field(init=False, repr=False)
    details: dict = attrs.field(init=False, repr=False)

    def __attrs_post_init__(self) -> None:
        self.err_type = type(self.error)
        self.err_msg = str(self.error)
        self.details = self.extract_details(self.error.__traceback__)

    def extract_details(self, tb: TracebackType) -> list[dict[str, Any]]:
        trace_info = []
        while tb:
            frame = tb.tb_frame
            trace_info.append(
                {
                    "file": frame.f_code.co_filename,
                    "func": frame.f_code.co_name,
                    "line_no": tb.tb_lineno,
                    "locals": frame.f_locals,
                },
            )
            tb = tb.tb_next
        return trace_info

    def is_ok(self) -> Literal[False]:
        return False

    def map(self, _: Callable[[T], U]) -> Self:
        return self

    def flatten(self) -> Self:
        return self

    def bind(self, _: Callable[[T], U]) -> Self:
        return self


Result = Ok | Err


def safe(func: callable) -> callable:
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Result:  # noqa: ANN002 ANN003
        try:
            return Ok(func(*args, **kwargs))
        except Exception as e:  # noqa: BLE001
            return Err((args, kwargs), e)

    return wrapper
