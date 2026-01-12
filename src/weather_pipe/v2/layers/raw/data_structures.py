from __future__ import annotations

from typing import Self

import attrs


@attrs.define(frozen=True)
class ApiConfig:
    api_key: str = attrs.field(repr=False, validator=attrs.validators.instance_of(str))
    location: str = attrs.field(validator=attrs.validators.instance_of(str))
    request_type: str = attrs.field(validator=attrs.validators.instance_of(str))

    @classmethod
    def from_config(cls, config: dict, api_key: str) -> Self:
        return cls(
            api_key=api_key,
            location=config.get("api_config", {}).get("location"),
            request_type=config.get("api_config", {}).get("request_type"),
        )

    def to_str(self) -> str:
        return f"http://api.weatherapi.com/v1/{self.request_type}.json?key={self.api_key}&q={self.location}&aqi=no"
