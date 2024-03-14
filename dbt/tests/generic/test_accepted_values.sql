-- Override built-in accepted_values generic so that it can return extra
-- columns for debugging
{% test accepted_values(
    model, column_name, values, quote=True, additional_select_columns=[]
) %}

    {%- set columns_csv = format_additional_select_columns(additional_select_columns) %}

    select {{ column_name }}{%- if columns_csv %}, {{ columns_csv }}{% endif %}
    from {{ model }}
    where
        {{ column_name }} not in (
            {% for value in values -%}
                {% if quote -%}'{{ value }}'
                {%- else -%}{{ value }}
                {%- endif -%}
                {%- if not loop.last -%},{%- endif %}
            {%- endfor %}
        )

{% endtest %}
