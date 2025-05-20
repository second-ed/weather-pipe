from collections import deque
from collections.abc import Sequence

import attrs

from weather_pipe.events import Event, parse_event
from weather_pipe.handlers import EventHandlers


@attrs.define
class MessageBus:
    event_handlers: EventHandlers = attrs.field()
    uows: dict = attrs.field()
    queue: deque = attrs.field(default=attrs.Factory(deque))

    def add_events(self, events: Sequence[Event]):
        if not isinstance(events, Sequence) or not all(isinstance(evt, Event) for evt in events):
            raise ValueError(f"{events} must be a Sequence of Event types")
        self.queue.extend(events)

    def handle_event(self):
        event = self.queue.popleft()
        handler = self.event_handlers[type(event)]
        uow = self.uows[type(event)]
        result = handler(event, uow)

        if isinstance(result, dict):
            result = parse_event(result)

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

    def handle_events(self):
        while self.queue:
            self.handle_event()
