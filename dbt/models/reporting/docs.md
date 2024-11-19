# assessment_progress

{% docs table_assessment_progress %}
Table for reporting current or final AV stats - depending on whether a township
is open - and the proportion of parcels that have been valued per major class
group, township, assessment stage, and year. Feeds public reporting assets.

**Primary Key**: `year`, `stage_name`, `geo_id`
{% enddocs %}

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

# vw_pin_school_impact

{% docs view_vw_pin_school_impact %}
View to get the 10 highest AVs by school district taxing agency and year.

**Primary Key**: `year`, `agency_num`, `pin`
{% enddocs %}

# vw_pin_township_class

{% docs view_vw_pin_township_class %}
View that provides pre-constructed common grouping columns across reporting
views.

**Primary Key**: `year`, `pin`
{% enddocs %}

# vw_pin_value_long

{% docs view_vw_pin_value_long %}
Assessed and market values by PIN and year, for each assessment stage.

The assessment stages are:

1. `PRE-MAILED` - Provisional values that are slated to be mailed by the
   Assessor once first-pass desk review completes
2. `MAILED` - Values initially mailed by the Assessor
3. `ASSESSOR PRE-CERTIFIED` - Provisional values that are slated to be set by
   the Assessor once appeals are finished
4. `ASSESSOR CERTIFIED` - Values after the Assessor has finished processing
   appeals
5. `BOARD CERTIFIED` - Values after the Board of Review has finished their
   appeals

### Assumptions

- Market value (`_mv`) columns accurately reflect incentives, statute,
  levels of assessment, building splits, etc.

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

**Primary Key**: `year`, `township`, `pin`
{% enddocs %}

# vw_top_5_muni

{% docs view_vw_top_5_muni %}
View to fetch the top five largest assessed values in a given municipality
by year.

**Primary Key**: `year`, `municipality`, `pin`
{% enddocs %}

# vw_town_sale_history

{% docs view_vw_town_sale_history %}
View for township-level reporting on sales by year.
Feeds public reporting assets.

**Primary Key**: `year`, `geography_type`, `geography_id`, `property_group`
{% enddocs %}
