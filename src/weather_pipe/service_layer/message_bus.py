from collections import deque
from collections.abc import Sequence

import attrs
from attrs.validators import instance_of

from weather_pipe.service_layer.uow import UnitOfWorkProtocol
from weather_pipe.usecases.events import Event
from weather_pipe.usecases.handlers import EventHandlers


@attrs.define
class MessageBus:
    event_handlers: EventHandlers = attrs.field()
    uows: dict[Event, UnitOfWorkProtocol] = attrs.field(validator=instance_of(dict))
    queue: deque = attrs.field(default=attrs.Factory(deque))

    def add_events(self, events: Sequence[Event]) -> None:
        if not isinstance(events, Sequence) or not all(isinstance(evt, Event) for evt in events):
            msg = f"{events} must be a Sequence of Event types"
            raise ValueError(msg)
        self.queue.extend(events)

    def handle_event(self) -> None:
        event = self.queue.popleft()
        uow = self.uows[type(event)]
        result = self.event_handlers[type(event)](event, uow)

        if isinstance(result, Event):
            if result.priority_event:
                self.queue.appendleft(result)
            else:
                self.queue.append(result)

        elif isinstance(result, Sequence):
            front, back = [], []

            for evt in result:
                if isinstance(evt, Event):
                    if evt.priority_event:
                        front.append(evt)
                    else:
                        back.append(evt)

            if front:
                self.queue.extendleft(reversed(front))
            if back:
                self.queue.extend(back)

    def handle_events(self) -> None:
        while self.queue:
            self.handle_event()
