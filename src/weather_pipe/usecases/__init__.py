from collections.abc import Sequence
from typing import Callable

from weather_pipe.usecases import events, handlers

EventHandlers = dict[type, Callable[[events.Event], events.Event | Sequence[events.Event] | None]]

EVENT_HANDLERS = {
    events.IngestToRawZone: handlers.raw_layer_handler,
    events.PromoteToBronzeLayer: handlers.bronze_layer_handler,
}
