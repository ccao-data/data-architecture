-- Test that row counts are equal to or above a certain value.
-- Note that `above` is greater-than-or-equal-to, so equal values will pass.
{% test row_count(model, column_name, above=none, equals=none) %}
    {%- if above is none and equals is none -%}
        {{
            exceptions.raise_compiler_error(
                "One of the arguments 'above' or 'equals' is required"
            )
        }}
    {%- elif above is not none and equals is not none -%}
        {{
            exceptions.raise_compiler_error(
                "The arguments 'above' or 'equals' cannot both be set"
            )
        }}
    {%- endif -%}
    {%- set comparison_operator = "<" if above is not none else "!=" -%}
    {%- set comparison_value = above or equals -%}
    {%- if column_name is defined %} {%- set count_col = column_name %}
    {%- else %} {%- set count_col = "*" %}
    {%- endif %}

    select row_count
    from (select count({{ count_col }}) as row_count from {{ model }})
    where row_count {{ comparison_operator }} {{ comparison_value }}
{% endtest %}
