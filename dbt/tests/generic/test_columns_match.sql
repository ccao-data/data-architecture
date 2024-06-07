-- Confirm that columns in the same table have the same value
{% test columns_match(
    model, column_name, matching_column_names, additional_select_columns=[]
) %}
    {%- set columns_csv = matching_column_names | join(", ") -%}
    {%- set columns_csv = column_name ~ ", " ~ columns_csv -%}
    {%- if additional_select_columns -%}
        {%- set additional_select_columns_csv = format_additional_select_columns(
            additional_select_columns
        ) -%}
        {%- set columns_csv = columns_csv ~ ", " ~ additional_select_columns_csv -%}
    {%- endif -%}

    select {{ columns_csv }}
    from {{ model }}
    where
        {%- for column in matching_column_names %}
            {{ column_name }} != {{ column }} {%- if not loop.last -%} and{% endif %}
        {%- endfor %}
{% endtest %}
