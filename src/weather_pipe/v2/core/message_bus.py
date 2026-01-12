from collections import deque
from collections.abc import Sequence
from typing import Self

import attrs
from attrs.validators import instance_of
from danom import Result
from io_adapters import IoAdapter

from weather_pipe.v2.core.event import Event, EventHandlers
from weather_pipe.v2.core.logger import logger


@attrs.define
class MessageBus:
    event_handlers: EventHandlers = attrs.field(validator=instance_of(dict))
    adapter: IoAdapter = attrs.field(validator=instance_of(IoAdapter))
    queue: deque = attrs.field(factory=deque)

    def add_events(self, events: Sequence[Event]) -> None:
        if not isinstance(events, Sequence) or not all(isinstance(evt, Event) for evt in events):
            msg = f"{events} must be a Sequence of Event types"
            raise ValueError(msg)
        self.queue.extend(events)
        return self

    def handle_events(self) -> Self:
        while self.queue:
            event = self.queue.popleft()
            logger.info(f"Popped new {event = }")
            result = self.event_handlers[type(event)](event, self.adapter)
            logger.info(f"Handled {event = } {result = }")

            if isinstance(result, Event):
                self.queue.append(result)
            elif isinstance(result, Sequence):
                self.queue.extend(result)
            elif isinstance(result, Result) and not result.is_ok():
                logger.error(f"{result.error = }")
                result.unwrap()

        return self
