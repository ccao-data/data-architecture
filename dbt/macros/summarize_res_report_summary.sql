-- Macro that takes a model and returns its row count grouped and
-- sorted by a given column. The sort order for the results can be specified
-- with the `ordering` argument, defaulting to "asc".
--
-- If the `print` argument is set to True (default is False), the macro will
-- print the results of the query to stdout, which allows this macro to be used
-- by scripts to return data.
{% macro summarize_res_report_summary(geo_type, group_prop, group_by) %}
    select
        triad,
        '{{ geo_type }}' AS geography_type,
        {% if group_prop %}
        property_group,
        {% else %}
        'ALL REGRESSION' as property_group,
        {% endif %}
        assessment_stage,
        {% if geo_type == "Town" %}
        township_code as geography_id,
        {% elif geo_type == "TownNBHD" %}
        townnbhd as geography_id,
        {% endif %}
        year,
        APPROX_PERCENTILE(total, 0.5) as fmv_median,
        COUNT(*) as pin_n,
        APPROX_PERCENTILE(total_land_sf, 0.5) as land_sf_median,
        APPROX_PERCENTILE(total_bldg_sf, 0.5) as bldg_sf_median,
        APPROX_PERCENTILE(yrblt, 0.5) as yrblt_median
    from all_values
    group by {{ group_by }}
{% endmacro %}
