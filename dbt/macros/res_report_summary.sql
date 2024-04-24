-- Macros that takes summarize FMVs and sales by different groupings for
-- reporting.res_report_summary.
{% macro res_report_summarize_values(geo_type, prop_group) %}
    select
        triad,
        '{{ geo_type }}' as geography_type,
        {% if prop_group %} property_group,
        {% elif not prop_group %} 'ALL REGRESSION' as property_group,
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
        {% elif not prop_group %} year
        {% endif %}
{% endmacro %}

{% macro res_report_summarize_sales(geo_type, prop_group) %}
    select
        {% if geo_type == "Town" %} township_code as geography_id,
        {% elif geo_type == "TownNBHD" %} townnbhd as geography_id,
        {% endif %}
        sale_year,
        {% if prop_group %} property_group,
        {% elif not prop_group %} 'ALL REGRESSION' as property_group,
        {% endif %}
        min(sale_price) as sale_min,
        approx_percentile(sale_price, 0.5) as sale_median,
        max(sale_price) as sale_max,
        count(*) as sale_n
    from sales
    group by
        {% if geo_type == "Town" %} township_code,
        {% elif geo_type == "TownNBHD" %} townnbhd,
        {% endif %}
        {% if prop_group %} sale_year, property_group
        {% elif not prop_group %} sale_year
        {% endif %}
{% endmacro %}
