# vw_assessment_card

{% docs view_pinval_vw_assessment_card %}
Table that holds card level data for subject PINs for PINVAL.

This table contains all PINs for the assessment data year for every
PINVAL-enabled model run, not just the PINs that have values and comps.
This choice enables us to support the generation of "error pages" for
non-tri or non-regression-class PINs that explain to a user why those PINs
do not have reports. Use the `is_report_eligible` column to filter for
report-eligible PINs, and use the `reason_report_ineligible` column to explain
missing reports.

In general, parcel-level attributes that come from `default.vw_pin_universe`
should have a `parcel_` prefix and should always be non-null. Card-level
attributes that come from `model.assessment_card` should have one of the
standard modeling prefixes and may be null. Standard modeling prefixes include:

- `meta_`
- `pred_`
- `char_`
- `loc_`
- `prox_`
- `acs5_`
- `other_`
- `time_`
- `ind_`
- `lag_`
- `ccao_`
- `shp_`
- `flag_`

See the docs for `model.assessment_card` for a detailed list of modeling
attributes.

**Primary Key**: `run_id`, `pin`, `meta_card_num`
{% enddocs %}

# vw_comp

{% docs view_pinval_vw_comp %}
Table that holds card level data for comparables for PINVAL.

**Primary Key**: `run_id`, `pin`, `meta_card_num`, `comp_num`
{% enddocs %}
