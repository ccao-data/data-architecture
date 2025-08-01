# vw_assessment_card

{% docs view_pinval_vw_assessment_card %}
Table that holds card level data for subject PINs for PINVAL.

This table contains all PINs for every PINVAL-enabled assessment data year,
not just the PINs that have values and comps. This choice enables us to support
the generation of "error pages" for non-tri or non-regression-class PINs that
explain to a user why those PINs do not have reports. Use the
`is_report_eligible` column to filter for report-eligible PINs, and use the
`reason_report_ineligible` column to explain missing reports.

While `model.assessment_card` uses `run_id` in its primary key, this view
instead uses `assessment_year`, since there can be multiple final models
per assessment year. For PINs that are in the assessment triad for a given
assessment year, `run_id` will correspond to the final model run ID for the
PIN's township.

**Primary Key**: `assessment_year`, `meta_pin`, `meta_card_num`
{% enddocs %}

# vw_comp

{% docs view_pinval_vw_comp %}
Table that holds card level data for comparables for PINVAL.

While `model.comp` uses `run_id` in its primary key, this view
instead uses `assessment_year` in order to make joins to
`pinval.vw_assessment_card` easier. Run IDs are not guaranteed
to match between this view and `pinval.vw_assessment_card` because
assessment years can have canonical comp runs that differ from their final
model runs.

**Primary Key**: `assessment_year`, `pin`, `meta_card_num`, `comp_num`
{% enddocs %}
