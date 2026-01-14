{{ config(materialized="incremental", unique_key="id") }}

{% if is_incremental() %}
    -- only insert new rows
    {{
        build_dim_hash_id_multiple(
            ref("bronze"), ["cond_text", "cond_icon", "cond_code"]
        )
    }}
    where id not in (select id from {{ this }})
{% else %}
    -- full-refresh
    {{
        build_dim_hash_id_multiple(
            ref("bronze"), ["cond_text", "cond_icon", "cond_code"]
        )
    }}
{% endif %}
