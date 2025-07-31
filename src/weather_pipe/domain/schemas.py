from datetime import datetime

import attrs
from attrs.validators import ge, instance_of, lt, max_len


@attrs.define
class BronzeForecast:
    time_epoch: int = attrs.field(validator=[instance_of(int)])
    time: str = attrs.field(validator=[instance_of(str)])
    temp_c: float = attrs.field(validator=[instance_of(float)])
    temp_f: float = attrs.field(validator=[instance_of(float)])
    is_day: int = attrs.field(validator=[instance_of(int)])
    code: int = attrs.field(validator=[instance_of(int)])
    wind_mph: float = attrs.field(validator=[instance_of(float)])
    wind_kph: float = attrs.field(validator=[instance_of(float)])
    wind_degree: int = attrs.field(validator=[instance_of(int)])
    pressure_mb: float = attrs.field(validator=[instance_of(float)])
    pressure_in: float = attrs.field(validator=[instance_of(float)])
    precip_mm: float = attrs.field(validator=[instance_of(float)])
    precip_in: float = attrs.field(validator=[instance_of(float)])
    snow_cm: float = attrs.field(validator=[instance_of(float)])
    humidity: int = attrs.field(validator=[instance_of(int)])
    cloud: int = attrs.field(validator=[instance_of(int)])
    feelslike_c: float = attrs.field(validator=[instance_of(float)])
    feelslike_f: float = attrs.field(validator=[instance_of(float)])
    windchill_c: float = attrs.field(validator=[instance_of(float)])
    windchill_f: float = attrs.field(validator=[instance_of(float)])
    heatindex_c: float = attrs.field(validator=[instance_of(float)])
    heatindex_f: float = attrs.field(validator=[instance_of(float)])
    dewpoint_c: float = attrs.field(validator=[instance_of(float)])
    dewpoint_f: float = attrs.field(validator=[instance_of(float)])
    will_it_rain: int = attrs.field(validator=[instance_of(int)])
    chance_of_rain: int = attrs.field(validator=[instance_of(int)])
    will_it_snow: int = attrs.field(validator=[instance_of(int)])
    chance_of_snow: int = attrs.field(validator=[instance_of(int)])
    vis_km: float = attrs.field(validator=[instance_of(float)])
    vis_miles: float = attrs.field(validator=[instance_of(float)])
    gust_mph: float = attrs.field(validator=[instance_of(float)])
    gust_kph: float = attrs.field(validator=[instance_of(float)])
    uv: float = attrs.field(validator=[instance_of(float)])
    row_guid: str = attrs.field(validator=[instance_of(str)])
    batch_guid: str = attrs.field(validator=[instance_of(str)])
    ingestion_datetime: datetime = attrs.field(validator=[instance_of(datetime)])
    text_id: int = attrs.field(validator=[instance_of(int)])
    wind_dir_id: int = attrs.field(validator=[instance_of(int)])
    icon_id: int = attrs.field(validator=[instance_of(int)])


@attrs.define
class BronzeWind:
    wind_dir_id: int = attrs.field(validator=[instance_of(int), ge(0), lt(21)])
    wind_dir: str = attrs.field(validator=[instance_of(str), max_len(30)])


@attrs.define
class BronzeDesc:
    text_id: int = attrs.field(validator=[instance_of(int), ge(0), lt(21)])
    text: str = attrs.field(validator=[instance_of(str), max_len(30)])


@attrs.define
class BronzeIcon:
    icon_id: int = attrs.field(validator=[instance_of(int), ge(0), lt(21)])
    icon: str = attrs.field(validator=[instance_of(str)])
