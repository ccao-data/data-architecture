# ratio_stats

{% docs table_ratio_stats %}
Table containing IAAO ratio statistics for various geographies.

Constructed daily by the ratio stats Glue job. Used to feed public
reporting Tableau breakouts.

**Primary Key**: `year`, `geography_type`, `geography_id`, `assessment_stage`,
`property_group`
{% enddocs %}

# res_report_summary

{% docs table_res_report_summary %}
Materialized version of `reporting.vw_res_report_summary`.

Materialized to speed up queries for Tableau.

**Primary Key**: `year`, `geography_type`, `geography_id`, `assessment_stage`,
`property_group`
{% enddocs %}

# vw_assessment_roll

{% docs view_vw_assessment_roll %}
View for township-level assessment reports at each stage, specifically used
for getting total AVs per class.

{% enddocs %}

# vw_pin_most_recent_boundary

{% docs view_vw_pin_most_recent_boundary %}

{% enddocs %}

# vw_pin_most_recent_sale

{% docs view_vw_pin_most_recent_sale %}

{% enddocs %}

# vw_ratio_stats

{% docs view_vw_ratio_stats %}
View to get the most recent sale for each PIN.

PINs without sales have `NULL` sale values.

**Primary Key**: `year`, `pin`
{% enddocs %}

# vw_res_report_summary

{% docs view_vw_res_report_summary %}
Aggregates statistics on characteristics, classes, AVs, and sales by
assessment stage, property groups, year, and various geographies.

**Primary Key**: `year`, `geography_type`, `geography_id`, `assessment_stage`,
`property_group`
{% enddocs %}

# vw_top_5

{% docs view_vw_top_5 %}

{% enddocs %}

# vw_town_sale_history

{% docs view_vw_town_sale_history %}
View for township-level reporting on sales by year.

{% enddocs %}
