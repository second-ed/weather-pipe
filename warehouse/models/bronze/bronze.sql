{{ config(materialized="incremental", unique_key="fact_id") }}

select {{ standardise_str_cols(ref("raw_to_bronze")) }}
from {{ ref("raw_to_bronze") }}
