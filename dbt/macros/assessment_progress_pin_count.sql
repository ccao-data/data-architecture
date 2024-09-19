-- Macro that counts the number of the number of PINs with values and the total
-- number of PINs by a year, stage, and a given geography.
{% macro assessment_progress_pin_count(from, geo_type, column_name) %}
    select
        '{{ geo_type }}' as geo_type,
        {{ column_name }} as geo_id,
        year,
        stage_name,
        stage_num,
        count(*) as num_pin_total,
        sum(has_value) as num_pin_w_value,
        sum(bldg) as bldg_sum,
        cast(approx_percentile(bldg, 0.5) as int) as bldg_median,
        sum(land) as land_sum,
        cast(approx_percentile(land, 0.5) as int) as land_median,
        sum(tot) as tot_sum,
        cast(approx_percentile(tot, 0.5) as int) as tot_median
    from {{ from }}
    where {{ column_name }} is not null and {{ column_name }} != ''
    group by {{ column_name }}, year, stage_name, stage_num
{% endmacro %}
