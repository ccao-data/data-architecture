-- Test that the count of a given column is the same when grouped by another
-- col, for example that the number of distinct township codes is the same
-- across years.
--
-- The optional min_value and max_value parameters allow the caller to hardcode
-- a valid range for the counts. If neither of these parameters are present,
-- the test will default to checking that all counts are the same, regardless
-- of the count value
{% test count_is_consistent(
    model, column_name, group_column, min_value=None, max_value=None
) %}

    with
        counts as (
            select {{ group_column }}, count(distinct({{ column_name }})) as cnt
            from {{ model }}
            group by {{ group_column }}
        ),
        ranked_counts as (
            select {{ group_column }}, cnt, rank() over (order by cnt desc) as rnk
            from counts
        )
    select {{ group_column }}, cnt as count
    from ranked_counts
    -- If min and/or max value are present as params, check for a hardcoded
    -- range of values
    {% if min_value and max_value %}
        where not cnt between {{ min_value }} and {{ max_value }}
    {% elif min_value and not max_value %} where cnt < min_value
    {% elif not min_value and max_value %} where cnt > max_value
    {% else %}
        -- If all rows have the same count, they should all have a rank of 1 when
        -- ranked according to count
        where rnk > 1
    {% endif %}
    order by cnt desc
{% endtest %}
