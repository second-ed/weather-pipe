{{ config(materialized="table") }}

select {{ standardise_str_cols(ref("raw_to_bronze")) }}
from {{ ref("raw_to_bronze") }}
