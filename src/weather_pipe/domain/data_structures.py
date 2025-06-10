import attrs


@attrs.define
class ApiConfig:
    api_key: str = attrs.field(repr=False)
    location: str = attrs.field()
    request_type: str = attrs.field()
