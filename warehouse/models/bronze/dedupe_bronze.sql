{{ config(materialized='table') }}

select * exclude (_rn)
from {{ ref('raw_to_bronze') }}
where _rn = 1
