-- Test that the count of a given column is the same when grouped by another
-- col, for example that the number of distinct township codes is the same
-- across years
{% test count_is_consistent(model, group_column, count_column) %}

with counts as (
  select {{ group_column }}, count(distinct({{ count_column }})) as cnt
  from {{ model }}
  group by {{ group_column }}
),
ranked_counts as (
  select {{ group_column }}, cnt, rank() over (order by cnt desc) as rnk
  from counts
)
select {{ group_column }}, cnt as count
from ranked_counts
-- If all rows have the same count, they should all have a rank of 1 when
-- ranked according to count
where rnk > 1
order by cnt desc

{% endtest %}
