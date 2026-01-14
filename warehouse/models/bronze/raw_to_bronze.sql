{{ config(materialized="incremental", unique_key="time_epoch_location") }}

with
    staged as (
        select
            * exclude (sys_col_ingestion_datetime, filename),
            condition.text as cond_text,
            condition.icon as cond_icon,
            condition.code as cond_code,
            time_epoch || '_' || location as time_epoch_location,
            sys_col_ingestion_datetime::timestamptz at time zone 'UTC'
            as sys_col_ingestion_datetime,
            filename as sys_col_filename,
            row_number() over (
                partition by time_epoch_location
                order by sys_col_ingestion_datetime desc
            ) as _rn
        from read_parquet('./data/raw/liverpool/*.parquet', filename = true)
    ),
    deduped as (select * from staged where _rn = 1)

select * exclude(condition)
from deduped

{% if is_incremental() %}
    where time_epoch_location not in (select time_epoch_location from {{ this }})
{% endif %}
