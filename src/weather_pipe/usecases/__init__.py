from __future__ import annotations

from collections.abc import Sequence
from typing import Callable

from src.weather_pipe.usecases._event import Event
from weather_pipe.usecases import bronze_layer, raw_layer

EventHandlers = dict[type, Callable[[Event], Event | Sequence[Event] | None]]

EVENT_HANDLERS = {
    raw_layer.IngestToRawZone: raw_layer.raw_layer_handler,
    bronze_layer.PromoteToBronzeLayer: bronze_layer.bronze_layer_handler,
}


def parse_event(msg: dict | Event) -> Event:
    # can put this between each stage
    # and return early if not need to parse
    if isinstance(msg, Event):
        return msg

    # go from most specific to least
    match msg:
        case {"config_path": _, "api_key": _, "repo_root": _}:
            return raw_layer.IngestToRawZone.from_dict(msg)

        case {"priority_event": _}:
            return Event.from_dict(msg)

    raise ValueError(f"Invalid message {msg}")
