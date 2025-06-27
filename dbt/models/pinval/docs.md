# vw_assessment_card

{% docs view_pinval_vw_assessment_card %}
Table that holds card level data for subject PINs for PINVAL.

This table contains all PINs for the assessment data year for every
PINVAL-enabled model run, not just the PINs that have values and comps.
This choice enables us to support the generation of "error pages" for
non-tri or non-regression-class PINs explaining to the user why those PINs
do not have reports. Use the `is_report_eligible` column to filter for
report-eligible PINs, and use the `reason_report_ineligible` column to explain
missing reports.

**Primary Key**: `run_id`, `pin`, `meta_card_num`
{% enddocs %}

# vw_comp

{% docs view_pinval_vw_comp %}
Table that holds card level data for comparables for PINVAL.

**Primary Key**: `run_id`, `pin`, `meta_card_num`, `comp_num`
{% enddocs %}
