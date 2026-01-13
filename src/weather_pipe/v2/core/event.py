from __future__ import annotations

from collections.abc import Sequence
from typing import Callable, TypeAlias

import attrs
from io_adapters import IoAdapter


@attrs.define
class Event:
    @classmethod
    def from_dict(cls, msg: dict) -> Event:
        filtered = {f.name: msg[f.name] for f in attrs.fields(cls) if f.name in msg}
        return cls(**filtered)


EventHandler: TypeAlias = Callable[[Event, IoAdapter], Event | Sequence[Event] | None]

EventHandlers = dict[type, EventHandler]

EVENT_HANDLERS = {}


def register_event_handler(event: Event) -> Callable[[EventHandler], EventHandler]:
    def wrapper(func: EventHandler) -> EventHandler:
        EVENT_HANDLERS[event] = func
        return func

    return wrapper
