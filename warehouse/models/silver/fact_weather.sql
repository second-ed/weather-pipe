{{ config(materialized="incremental", unique_key="fact_id") }}

with
    base as (

        select
            *,
            {{ cast("time", dbt.type_timestamp()) }} as timestamp,
            date_part('hour', timestamp) as hour,
            date_part('month', timestamp) as month,

            hash(lower(trim(location))) as location_id,

            hash(
                lower(trim(cond_text))
                || '_'
                || lower(trim(cond_icon))
                || '_'
                || cast(cond_code as text)
            ) as condition_id,

            hash(lower(trim(wind_dir))) as wind_dir_id,

        from {{ ref("bronze") }}

        {% if is_incremental() %}
            where
                sys_col_ingestion_datetime
                > (select max(sys_col_ingestion_datetime) from {{ this }})
        {% endif %}

    )

select * exclude(location, cond_code, cond_icon, cond_text, wind_dir)
from base
