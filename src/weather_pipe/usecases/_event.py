from __future__ import annotations

import attrs


@attrs.define
class Event:
    priority_event: bool = attrs.field(default=False)

    @classmethod
    def from_dict(cls, msg: dict) -> Event:
        filtered = {f.name: msg[f.name] for f in attrs.fields(cls) if f.name in msg}
        return cls(**filtered)
