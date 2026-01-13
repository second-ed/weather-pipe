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
