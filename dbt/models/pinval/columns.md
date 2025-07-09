## is_report_eligible

{% docs column_pinval_is_report_eligible %}
When `TRUE`, this PIN is eligible for a PINVAL report for the given model run
{% enddocs %}

## meta_card_num

{% docs column_pinval_meta_card_num %}
The card number for the card.

There are two cases in which this column might be null:

- **No entry exists for this card in `model.assessment_card`**. This
  means that the card was ineligible for a model value and got filtered out of
  the model's assessment set prior to prediction. In this case, the `meta_pin`
  column (and all other columns that come from `model.assessment_card`) will
  also be null.
- **The PIN does not have any cards**. This indicates a data error that
  causes the model to ignore the parcel for valuation purposes. In this case,
  the `reason_report_ineligible` column will have the value `'missing_card'`.

{% enddocs %}

## run_id

{% docs column_pinval_run_id %}
Run ID for the model run associated with this card and its values.

In the case of a parcel that is ineligible for a PINVAL report, the presence
of a value for this column might seem confusing because that parcel wasn't
actually valued in the model run. However, since this table requires a row
for every parcel in every eligible model run in order to compute parcel
eligibility in the `is_report_eligible` and `reason_report_ineligible` columns,
we add parcels to model runs that considered them ineligible. As a result,
every model run in this table should have a row for every parcel in its data
year, regardless of whether that parcel was actually part of the model run.
{% enddocs %}

## char_class

{% docs column_pinval_char_class %}
The class for the card or parcel that this row represents.

If a row represents a card that was part of the model assessment set, then
this column will be the card class that we used as a predictor in the model.
If a row instead represents a parcel that was _not_ part of the assessment set,
then this column will be the parcel class, and is used in the
`reason_report_ineligible` column to explain why we excluded the parcel from
the assessment set.

This column will never be null.
{% enddocs %}

## char_class_desc

{% docs column_pinval_char_class_desc %}
A short description explaining the code contained in `char_class`
{% enddocs %}

## reason_report_ineligible

{% docs column_pinval_reason_report_ineligible %}
A code indicating why this PIN is ineligible for a PINVAL report for the given
model run.

Possible values for this variable are:

- `'condo'`: The PIN is a condominium unit, which we value with a separate
  valuation model
- `'non_regression_class'`: The PIN is not a class of property that we value with
  the residential valuation model
    - Condo units have their own code (`'condo'`) because they are a special case
      in which a user might be particularly confused by the absence of a report
- `'non_tri'`: The PIN is not in the reassessment triad for the assessment year
- `'unknown'`: The PIN is in some sort of unexpected state such that we can't
  explain why it's ineligible. This value primarily exists to allow us to test
  for unexpected conditions in our data integrity tests, and should never
  be present in the data in practice
- `'missing_card'`: The PIN does not have any cards. This usually indicates an
  error in the underlying source data that causes the model to ignore
  the parcel for valuation purposes.
- `NULL`: The PIN is eligible for a PINVAL report. This should only ever be
  the case when `is_report_eligible` is `TRUE`, and our data integrity
  tests check to make sure this is true
{% enddocs %}
