-- Override dbt_utils.sequential_values to support `additional_select_columns` parameter
{% test sequential_values(
    model,
    column_name,
    interval=1,
    datepart=one,
    group_by_columns=[],
    additional_select_columns=[]
) %}

    {%- set previous_column_name = "previous_" ~ slugify(column_name) -%}

    {%- if group_by_columns | length() > 0 -%}
        {%- set select_gb_cols = group_by_columns | join(",") -%}
        {%- set partition_gb_cols = "partition by " + group_by_columns | join(",") -%}
    {%- endif -%}

    with
        windowed as (

            select
                {{ select_gb_cols }},
                {{ column_name }},
                lag({{ column_name }}) over (
                    {{ partition_gb_cols }} order by {{ column_name }}
                ) as {{ previous_column_name }},
                {{ format_additional_select_columns(additional_select_columns) }}
            from {{ model }}
        ),

        validation_errors as (
            select *
            from windowed
            {% if datepart %}
                where
                    not (
                        cast({{ column_name }} as {{ dbt.type_timestamp() }}) = cast(
                            {{ dbt.dateadd(datepart, interval, previous_column_name) }}
                            as {{ dbt.type_timestamp() }}
                        )
                    )
            {% else %}
                where
                    not (
                        {{ column_name }} = {{ previous_column_name }} + {{ interval }}
                    )
            {% endif %}
        )

    select *
    from validation_errors

{% endtest %}
