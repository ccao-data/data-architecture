# ratio_stats

{% docs table_ratio_stats %}
Table containing IAAO ratio statistics for various geographies.

Constructed daily by the ratio stats Glue job. Used to feed public
reporting Tableau dashboards.

**Primary Key**: `year`, `geography_type`, `geography_id`, `assessment_stage`,
`property_group`
{% enddocs %}

# ratio_stats_input

{% docs table_ratio_stats_input %}
Table to feed the Python dbt job that creates the `reporting.ratio_stats` table.
Feeds public reporting assets.

**Primary Key**: `year`, `pin`, `assessment_stage`
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
View for reporting total AVs and PIN counts per major class group, township,
assessment stage, and year. Feeds public reporting assets.

**Primary Key**: `year`, `township_name`, `class`, `stage`
{% enddocs %}

# vw_assessment_roll_muni

{% docs view_vw_assessment_roll_muni %}
View for reporting total AVs and PIN counts per major class group, municipality,
assessment stage, and year. Feeds public reporting assets.

**Primary Key**: `year`, `municipality_name`, `class`, `stage`
{% enddocs %}

# vw_pin_most_recent_boundary

{% docs view_vw_pin_most_recent_boundary %}
View joining PINs to the most recent available political boundaries.

Used for outreach and reporting.

**Primary Key**: `pin10`
{% enddocs %}

# vw_pin_most_recent_sale

{% docs view_vw_pin_most_recent_sale %}
View to get the most recent sale for each PIN.

PINs without sales have `NULL` sale values.

**Primary Key**: `year`, `pin`
{% enddocs %}

# vw_pin_township_class

{% docs view_vw_pin_township_class %}
View that provides pre-constructed common grouping columns across reporting
views.

**Primary Key**: `year`, `pin`
{% enddocs %}

# vw_pin_value_long

{% docs view_vw_pin_value_long %}
Assessed values by PIN and year, for each assessment stage.

The assessment stages are:

1. `mailed` - Values initially mailed by the Assessor
2. `certified` - Values after the Assessor has finished processing appeals
2. `board` - Values after the Board of Review has finished their appeals

### Assumptions

- Taking an arbitrary value by 14-digit PIN and year is sufficient for accurate
  values. We do this because even given the criteria to de-dupe `asmt_all`,
  we still end up with duplicates by PIN and year.

**Primary Key**: `year`, `pin`, `stage_name`
{% enddocs %}

# vw_res_report_summary

{% docs view_vw_res_report_summary %}
Aggregates statistics on characteristics, classes, AVs, and sales
by assessment stage, property groups, year, and various geographies.
Feeds public reporting assets.

**Primary Key**: `year`, `geography_type`, `geography_id`, `assessment_stage`,
`property_group`
{% enddocs %}

# vw_top_5

{% docs view_vw_top_5 %}
View to fetch the top five largest assessed values in a given township
by year.

**Primary Key**: `year`, `township`, `parid`
{% enddocs %}

# vw_top_5_muni

{% docs view_vw_top_5_muni %}
View to fetch the top five largest assessed values in a given municipality
by year.

**Primary Key**: `year`, `municipality`, `parid`
{% enddocs %}

# vw_town_sale_history

{% docs view_vw_town_sale_history %}
View for township-level reporting on sales by year.
Feeds public reporting assets.

**Primary Key**: `year`, `geography_type`, `geography_id`, `property_group`
{% enddocs %}
