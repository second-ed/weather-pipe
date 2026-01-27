{{ config(materialized="view") }}


select
    f.month,
    d.location,
    avg(f.temp_c) as temp_c,
    avg(f.wind_kph) as wind_kph,
    avg(f.gust_kph) as gust_kph,
    avg(f.wind_degree) as wind_degree,
    avg(f.precip_mm) as precip_mm,
    avg(f.chance_of_rain) as chance_of_rain,
    avg(f.snow_cm) as snow_cm,
    avg(f.chance_of_snow) as chance_of_snow,
    avg(f.humidity) as humidity,
    avg(f.cloud) as cloud,
    avg(f.feelslike_c) as feelslike_c,
    avg(f.windchill_c) as windchill_c,
    avg(f.heatindex_c) as heatindex_c,
    avg(f.dewpoint_c) as dewpoint_c,
    avg(f.vis_km) as vis_km
from {{ ref("fact_weather") }} f
join {{ ref("dim_location") }} d on f.location_id = d.id
group by f.month, d.location
order by d.location, f.month
