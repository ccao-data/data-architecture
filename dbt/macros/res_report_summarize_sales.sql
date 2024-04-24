-- Macro that takes a model and returns its row count grouped and
-- sorted by a given column. The sort order for the results can be specified
-- with the `ordering` argument, defaulting to "asc".
--
-- If the `print` argument is set to True (default is False), the macro will
-- print the results of the query to stdout, which allows this macro to be used
-- by scripts to return data.
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
