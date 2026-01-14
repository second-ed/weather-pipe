{% macro standardise_str_cols(relation) %}
    {% set cols = adapter.get_columns_in_relation(relation) %}
    {% set select_list = [] %}

    {% for col in cols %}
        {% if (
            col.data_type.lower().startswith("character varying")
            or col.data_type.lower().startswith("text")
            or col.data_type.lower().startswith("varchar")
        ) and not col.name.startswith("sys_col_") %}
            {% do select_list.append("lower(trim(" ~ col.name ~ ")) as " ~ col.name) %}
        {% else %} {% do select_list.append(col.name) %}
        {% endif %}
    {% endfor %}

    {{ return(select_list | join(", ")) }}
{% endmacro %}


{% macro build_dim_hash_id_multiple(table_ref, col_names) %}
    select
        hash(
            {% for col in col_names %}
                lower(trim(cast({{ col }} as text))){{ " || '_' || " if not loop.last }}
            {% endfor %}
        ) as id,
        {% for col in col_names %}
            {{ col }} as {{ col }}{% if not loop.last %}, {% endif %}
        {% endfor %}
    from
        (
            select distinct
                {% for col in col_names %}
                    {{ col }} as {{ col }}{% if not loop.last %}, {% endif %}
                {% endfor %}
            from {{ table_ref }}
        ) as distinct_values
{% endmacro %}
