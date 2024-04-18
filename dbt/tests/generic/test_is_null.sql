-- Confirm that a column is null. Essentially, the opposite of the `not_null`
-- test.
{% test is_null(model, column_name, additional_select_columns=[]) %}

    select
        {% if additional_select_columns -%}
            {{ format_additional_select_columns(additional_select_columns) }},
        {%- endif %}
        {{ column_name }}
    from {{ model }}
    where {{ column_name }} is not null

{% endtest %}
