-- Test that row values match after joining two tables. Row values can be
-- a subset of the values in the joined table, i.e. if the class of PINA
-- from tableA is '212' and the class of PINA from tableB is '211' and '212',
-- then tableA matches (returns no rows).
{% test row_values_match_after_join(
    model,
    column,
    external_model,
    external_column,
    column_name="model_col",
    external_column_name="external_model_col",
    join_columns=[],
    select_columns=[]
) %}

    {%- set join_columns_csv = join_columns | join(", ") -%}
    {%- set select_columns_csv = select_columns | join(", ") -%}

    {%- if "." in column -%} {%- set model_col = column -%}
    {%- else -%} {%- set model_col = "model" ~ "." ~ column -%}
    {%- endif -%}

    {%- if "." in external_column -%} {%- set external_model_col = external_column -%}
    {%- else -%}
        {%- set external_model_col = "external_model" ~ "." ~ external_column -%}
    {%- endif -%}

    select
        {{ join_columns_csv }},
        {% if select_columns_csv %} {{ select_columns_csv }},{% endif %}
        array_agg({{ model_col }}) as {{ column_name }},
        array_agg({{ external_model_col }}) as {{ external_column_name }}
    from {{ external_model }} as external_model
    join (select * from {{ model }}) as model using ({{ join_columns_csv }})
    group by {{ join_columns_csv }}
    having
        sum(case when {{ external_model_col }} = {{ model_col }} then 1 else 0 end) = 0

{% endtest %}
