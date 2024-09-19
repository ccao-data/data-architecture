-- Macros that summarize FMVs and sales by different groupings specifically for
-- reporting.res_report_summary.
{% macro res_report_summarize_values(from, geo_type, prop_group) %}
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
        approx_percentile(yrblt, 0.5) as yrblt_median,
        -- Mode calculation trick from: https://stackoverflow.com/a/66954462
        map_keys(histogram(class))[
            array_position(
                map_values(histogram(class)), array_max(map_values(histogram(class)))
            )
        ] as class_mode
    from {{ from }}
    group by
        assessment_stage,
        triad,
        {% if geo_type == "Town" %} township_code,
        {% elif geo_type == "TownNBHD" %} townnbhd,
        {% endif %}
        year
        {% if prop_group %}, property_group{% endif %}
{% endmacro %}

{% macro res_report_summarize_sales(from, geo_type, prop_group) %}
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
    from {{ from }}
    group by
        {% if geo_type == "Town" %} township_code,
        {% elif geo_type == "TownNBHD" %} townnbhd,
        {% endif %}
        sale_year
        {% if prop_group %}, property_group{% endif %}
{% endmacro %}
