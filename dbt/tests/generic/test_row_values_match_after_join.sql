-- Test that row values match after joining two tables. Row values can be
-- a subset of the values in the joined table, i.e. if the class of PINA
-- from tableA is '212' and the class of PINA from tableB is '211' and '212',
-- then tableA matches (returns no rows).
{% test row_values_match_after_join(
    model,
    column_name,
    external_model,
    external_column_name,
    join_condition,
    group_by,
    column_alias="model_col",
    external_column_alias="external_model_col",
    additional_select_columns=[]
) %}

    {%- set additional_select_columns_csv = format_additional_select_columns(
        additional_select_columns
    ) -%}

    {%- set group_by_qualified = [] -%}
    {%- for col in group_by -%}
        {%- set _ = group_by_qualified.append("model." ~ col) -%}
    {%- endfor -%}
    {%- set group_by_csv = group_by_qualified | join(", ") -%}

    {%- if "." in column_name -%} {%- set model_col = column_name -%}
    {%- else -%} {%- set model_col = "model." ~ column_name -%}
    {%- endif -%}

    {%- if "." in external_column_name -%} {%- set external_model_col = external_column_name -%}
    {%- else -%}
        {%- set external_model_col = "external_model." ~ external_column_name -%}
    {%- endif -%}

    select
        {{ group_by_csv }},
        {% if additional_select_columns_csv -%}
            {{ additional_select_columns_csv }},
        {% endif %}
        array_agg({{ model_col }}) as {{ column_alias }},
        array_agg({{ external_model_col }}) as {{ external_column_alias }}
    from {{ external_model }} as external_model
    join (select * from {{ model }}) as model
        {{ join_condition }}
    group by {{ group_by_csv }}
    having
        sum(case when {{ external_model_col }} = {{ model_col }} then 1 else 0 end) = 0

{% endtest %}
