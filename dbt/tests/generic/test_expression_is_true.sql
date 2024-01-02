-- Filter for rows where a given `expression` is false.
--
-- Adapted from `dbt_utils.expression_is_true`, and extended to support
-- an optional `select_columns` option representing an array of columns to
-- select for failing rows in addition to `column_name`. If no `select_columns`
-- array is provided, defaults to selecting the column represented by
-- `column_name`; if `column_name` is also missing, falls back to selecting 1
-- for failing rows.
{% test expression_is_true(model, expression, column_name, select_columns=[]) %}
    {%- set select_columns_csv = select_columns | join(", ") -%}
    {%- if column_name -%}
        {%- set columns_csv = column_name -%}
        {%- if select_columns_csv -%}
            {%- set columns_csv = columns_csv ~ ", " ~ select_columns_csv -%}
        {%- endif -%}
    {%- elif select_columns_csv -%} {%- set columns_csv = select_columns_csv -%}
    {%- else -%} {%- set columns_csv = "1" -%}
    {%- endif -%}

    select {{ columns_csv }}
    from {{ model }}
    where not ({{ column_name }} {{ expression }})
{% endtest %}
