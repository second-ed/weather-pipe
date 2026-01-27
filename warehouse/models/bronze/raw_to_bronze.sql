{{ config(materialized="incremental", unique_key="fact_id") }}

with
    staged as (
        select
            hash(time_epoch || '_' || location) as fact_id,
            * exclude (sys_col_ingestion_datetime, filename),
            condition.text as cond_text,
            condition.icon as cond_icon,
            condition.code as cond_code,
            sys_col_ingestion_datetime::timestamptz at time zone 'UTC'
            as sys_col_ingestion_datetime,
            filename as sys_col_filename,
            row_number() over (
                partition by fact_id order by sys_col_ingestion_datetime desc
            ) as _rn
        from read_parquet('./data/raw/liverpool/*.parquet', filename = true)
    ),
    deduped as (select * from staged where _rn = 1)

select * exclude(condition)
from deduped

{% if is_incremental() %}
    where fact_id not in (select fact_id from {{ this }})
{% endif %}
