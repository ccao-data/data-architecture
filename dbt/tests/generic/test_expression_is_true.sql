-- Filter for rows where a given `expression` is false.
--
-- Adapted from `dbt_utils.expression_is_true`, and extended to support
-- an optional `select_columns` option representing an array of columns to
-- select for failing rows. If no `select_columns` array is provided, defaults
-- to selecting 1 for failing rows.
{% test expression_is_true(model, expression, select_columns=[1]) %}
    {%- set columns_csv = select_columns | join(", ") %}

    select {{ columns_csv }}
    from {{ model }}
    where not ({{ expression }})
{% endtest %}
