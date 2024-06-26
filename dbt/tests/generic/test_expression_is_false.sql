-- Filter for rows where a given `expression` is true.
--
-- Adapted from our own `expression_is_true`. Supports an optional
-- `additional_select_columns` option representing an array of columns to select
-- for failing rows in addition to `column_name`. If no
-- `additional_select_columns` array is provided, defaults to selecting the
-- column represented by `column_name`; if `column_name` is also missing, falls
-- back to selecting 1 for failing rows.
{% test expression_is_false(
    model, column_name, expression, additional_select_columns=[]
) %}
    {%- set select_columns_csv = format_additional_select_columns(
        additional_select_columns
    ) -%}
    {%- if column_name -%}
        {%- set columns_csv = column_name -%}
        {%- if select_columns_csv -%}
            {%- set columns_csv = columns_csv ~ ", " ~ select_columns_csv -%}
        {%- endif -%}
    {%- elif select_columns_csv -%} {%- set columns_csv = select_columns_csv -%}
    {%- else -%} {%- set columns_csv = "1 AS fail" -%}
    {%- endif -%}

    select {{ columns_csv }}
    from {{ model }}
    where ({{ column_name }} {{ expression }})
{% endtest %}
