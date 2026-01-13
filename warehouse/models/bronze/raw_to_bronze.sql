{{ config(materialized="table") }}

select
    * exclude (sys_col_ingestion_datetime, filename),
    sys_col_ingestion_datetime::timestamptz at time zone 'UTC'
    as sys_col_ingestion_datetime,
    filename as sys_col_filename,
    row_number() over (
        partition by time_epoch, location
        order by sys_col_ingestion_datetime desc
    ) as _rn
from read_parquet('./data/raw/liverpool/*.parquet', filename = true)
