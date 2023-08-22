-- Test that a given set of columns are unique, with an optional
-- threshold indicating an allowable number of duplicates.
--
-- For example, test that a given PIN has been sold no more than
-- twice in one year.
--
-- The duplicate threshold defaults to 1, in which case this is a standard
-- uniqueness test.
--
-- Adapted from dbt_utils.unique_combination_of_columns, and adjusted to add the
-- optional duplicate threshold and to only report one row for each dupe.
{% test unique_combination_of_columns(
    model, combination_of_columns, allowed_duplicates=0, where=null
) %}

    {%- set columns_csv = combination_of_columns | join(", ") %}

    select {{ columns_csv }}, count(*) as num_duplicates
    from {{ model }}
    group by {{ columns_csv }}
    having count(*) > {{ allowed_duplicates }} + 1
    {%- if where -%} where ({{ where }}) {%- endif -%}

{% endtest %}
