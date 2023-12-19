-- Test that the a given expression returns a non-zero number of rows
{% test value_is_present(model, expression) %}

    select row_count
    from (select count(*) as row_count from {{ model }} where {{ expression }})
    where row_count = 0

{% endtest %}
