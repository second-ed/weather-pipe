{{ config(materialized="incremental", unique_key="id") }}

{% if is_incremental() %}
    {{ build_dim_hash_id_multiple(ref("bronze"), ["wind_dir"]) }}
    where id not in (select id from {{ this }})
{% else %} {{ build_dim_hash_id_multiple(ref("bronze"), ["wind_dir"]) }}
{% endif %}
