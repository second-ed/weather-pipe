{{ config(materialized="incremental", unique_key="time_epoch_location") }}

select {{ standardise_str_cols(ref("raw_to_bronze")) }}
from {{ ref("raw_to_bronze") }}
