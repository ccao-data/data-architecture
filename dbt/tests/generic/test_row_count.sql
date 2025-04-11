-- Test that row counts are above a certain value.
-- This includes an implicit test to pass when row counts match.
{% test row_count(model, column_name, above) %}
    {%- if column_name is defined %} {%- set count_col = column_name %}
    {%- else %} {%- set count_col = "*" %}
    {%- endif %}

    select row_count
    from (select count({{ count_col }}) as row_count from {{ model }})
    where row_count < {{ above }}
{% endtest %}
