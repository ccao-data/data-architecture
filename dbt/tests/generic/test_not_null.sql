-- Override built-in not_null generic so that it can return extra columns
-- for debugging
{% test not_null(model, column_name, additional_select_columns=[]) %}

    {%- set columns_csv = "*" %}
    {%- if additional_select_columns %}
        {%- set columns_csv = format_additional_select_columns(
            additional_select_columns
        ) %}
    {%- endif %}

    select {{ columns_csv }}
    from {{ model }}
    where {{ column_name }} is null

{% endtest %}
