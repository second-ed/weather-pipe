import attrs


@attrs.define
class Event:
    priority_event: bool = attrs.field(default=False)

    @classmethod
    def from_dict(cls, msg):
        filtered = {f.name: msg[f.name] for f in attrs.fields(cls) if f.name in msg}
        return cls(**filtered)


@attrs.define
class IngestToRawZone(Event):
    config_path: str = attrs.field(default="")
    repo_root: str = attrs.field(default="")
    api_key: str = attrs.field(default="", repr=False)


@attrs.define
class PromoteToBronzeLayer(Event):
    db_path: str = attrs.field(default="")


def parse_event(msg: dict | Event) -> Event:
    # can put this between each stage
    # and return early if not need to parse
    if isinstance(msg, Event):
        return msg

    # go from most specific to least
    match msg:
        case {"config_path": _, "api_key": _, "repo_root": _}:
            return IngestToRawZone.from_dict(msg)

        case {"priority_event": _}:
            return Event.from_dict(msg)

    raise ValueError(f"Invalid message {msg}")
