-- Override built-in not_null generic so that it can return extra columns
-- for debugging
{% test not_null(model, column_name, select_columns=[]) %}

    {%- set columns_csv = select_columns | join(", ") if select_columns else "*" %}

    select {{ columns_csv }}
    from {{ model }}
    where {{ column_name }} is null

{% endtest %}
