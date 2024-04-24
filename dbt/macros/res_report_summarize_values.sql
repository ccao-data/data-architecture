-- Macro that takes a model and returns its row count grouped and
-- sorted by a given column. The sort order for the results can be specified
-- with the `ordering` argument, defaulting to "asc".
--
-- If the `print` argument is set to True (default is False), the macro will
-- print the results of the query to stdout, which allows this macro to be used
-- by scripts to return data.
{% macro res_report_summarize_values(geo_type, prop_group) %}
    select
        triad,
        '{{ geo_type }}' as geography_type,
        {% if prop_group %} property_group,
        {% else %} 'ALL REGRESSION' as property_group,
        {% endif %}
        assessment_stage,
        {% if geo_type == "Town" %} township_code as geography_id,
        {% elif geo_type == "TownNBHD" %} townnbhd as geography_id,
        {% endif %}
        year,
        approx_percentile(total, 0.5) as fmv_median,
        count(*) as pin_n,
        approx_percentile(total_land_sf, 0.5) as land_sf_median,
        approx_percentile(total_bldg_sf, 0.5) as bldg_sf_median,
        approx_percentile(yrblt, 0.5) as yrblt_median
    from all_values
    group by
        assessment_stage,
        triad,
        {% if geo_type == "Town" %} township_code,
        {% elif geo_type == "TownNBHD" %} townnbhd,
        {% endif %}
        {% if prop_group %} year, property_group
        {% else %} year
        {% endif %}
{% endmacro %}
