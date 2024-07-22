-- Macros that summarize FMVs and sales by different groupings specifically for
-- reporting.res_report_summary.
{% macro assessment_progress_pin_count(from, geo_type, column_name) %}
    select
        '{{ geo_type }}' as geo_type,
        {{ column_name }} as geo_id,
        year,
        stage_name,
        stage_num,
        count(*) as total_n,
        sum(has_value) as num_pin_w_value
    from {{ from }}
    where {{ column_name }} is not null and {{ column_name }} != ''
    group by {{ column_name }}, year, stage_name, stage_num
{% endmacro %}
