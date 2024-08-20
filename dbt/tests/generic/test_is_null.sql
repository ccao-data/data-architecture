-- Confirm that a column is null. Essentially, the opposite of the `not_null`
-- test.
{% test is_null(model, column_name, additional_select_columns=[], anti_join={}) %}

    select
        {% if additional_select_columns -%}
            {{ format_additional_select_columns(additional_select_columns) }},
        {%- endif %}
        {{ column_name }}
    from
        (
            select model.*
            from (select * from {{ model }}) as model
            {% if anti_join -%}
                left join
                    {{ anti_join.table_name }} as anti_join
                    on model.{{ anti_join.join_on }} = anti_join.{{ anti_join.join_on }}
                where anti_join.{{ anti_join.join_on }} is null
            {%- endif %}
        ) as model
    where model.{{ column_name }} is not null

{% endtest %}
