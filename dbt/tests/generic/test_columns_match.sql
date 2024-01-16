-- Confirm that columns in the same table have the same value
{% test columns_match(model, column_name, columns, additional_select_columns=[]) %}
    {%- set columns_csv = columns | join(", ") -%}
    {%- set columns_csv = column_name ~ ", " ~ columns_csv -%}
    {%- if additional_select_columns -%}
        {%- set additional_select_columns_csv = additional_select_columns | join(
            ", "
        ) -%}
        {%- set columns_csv = columns_csv ~ ", " ~ additional_select_columns_csv -%}
    {%- endif -%}

    select {{ columns_csv }}
    from {{ model }}
    where
        {%- for column in columns %}
            {{ column_name }} != {{ column }} {%- if not loop.last -%} and{% endif %}
        {%- endfor %}
{% endtest %}
