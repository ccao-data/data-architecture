-- Test that one or more string columns do not contain extraneous whitespace
{% test no_extra_whitespace(model, column_names) %}

    {%- set columns = column_names | join(", ") %}

    {%- set conditions_list = [] %}
    {% for column_name in column_names %}
        {%- set conditions_list = conditions_list.append(
            "("
            + column_name
            + " like '%  %' or "
            + column_name
            + " like '% ' or "
            + column_name
            + " like ' %')"
        ) %}
    {%- endfor %}
    {%- set conditions = conditions_list | join(" or ") %}

    with
        validation_errors as (
            select {{ columns }} from {{ model }} where {{ conditions }}
        )
    select *
    from validation_errors

{% endtest %}
