-- Confirm that columns in the same table have the same value
{% test columns_match(model, column_name, columns, select_columns=[]) %}
    {%- set columns_csv = columns | join(", ") -%}
    {%- set columns_csv = column_name ~ ", " ~ columns_csv -%}
    {%- if select_columns -%}
        {%- set select_columns_csv = select_columns | join(", ") -%}
        {%- set columns_csv = columns_csv ~ ", " ~ select_columns_csv -%}
    {%- endif -%}

    select {{ columns_csv }}
    from {{ model }}
    where
        {%- for column in columns %}
            {{ column_name }} != {{ column }} {%- if not loop.last -%} and{% endif %}
        {%- endfor %}
{% endtest %}
