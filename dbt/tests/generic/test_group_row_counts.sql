-- Test that the a given expression returns a non-zero number of rows
{% test group_row_counts(model, group_by, condition) %}

    select *
    from ({{ row_count_by_group(model, group_by) }})
    where count {{ condition }}

{% endtest %}
