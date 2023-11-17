-- Test that row values match after joining two tables. Row values can be
-- a subset of the values in the joined table, i.e. if the class of PINA
-- from tableA is '212' and the class of PINA from tableB is '211' and '212',
-- then tableA matches (returns no rows).
{% test row_values_match_after_join(
    model, column, external_model, external_column, join_columns=[]
) %}

    {%- set join_columns_csv = join_columns | join(", ") -%}

    select
        {{ join_columns_csv }},
        array_agg(m.{{ column }}) as model_col,
        array_agg(em.{{ external_column }}) as ext_model_col
    from {{ external_model }} as em
    join (select * from {{ model }}) as m using ({{ join_columns_csv }})
    group by {{ join_columns_csv }}
    having
        sum(case when em.{{ external_column }} = m.{{ column }} then 1 else 0 end) = 0

{% endtest %}
