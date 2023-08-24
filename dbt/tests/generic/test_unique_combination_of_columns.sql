-- Test that a given set of columns are unique, with an optional
-- threshold indicating an allowable number of duplicates and an optional
-- filter clause to restrict the set of rows that should be unique.
--
-- For example, test that a given PIN has been sold no more than
-- twice in one year, for only active rows.
--
-- The duplicate threshold defaults to 1, in which case this is a standard
-- uniqueness test. The where clause defaults to null, which indicates
-- that the full set of rows should be unique on the given columns.
--
-- Adapted from dbt_utils.unique_combination_of_columns, and adjusted to add the
-- optional duplicate threshold, to add the optional where clause, and to only
-- report one row for each dupe.
{% test unique_combination_of_columns(
    model, combination_of_columns, allowed_duplicates=0, where=null
) %}

    {%- set columns_csv = combination_of_columns | join(", ") %}

    select {{ columns_csv }}, count(*) as num_duplicates
    from {{ model }}
    {% if where %} where ({{ where }}) {% endif %}
    group by {{ columns_csv }}
    having count(*) > {{ allowed_duplicates }} + 1

{% endtest %}
