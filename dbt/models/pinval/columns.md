## assessment_card_assessment_year

{% docs column_pinval_assessment_card_assessment_year %}
Assessment year for the model run associated with this card and its values.

An assessment year can map to multiple model runs, since model runs can be
different for different towns in a triad. However, cards should be unique
by assessment year in this view.
{% enddocs %}

## assessment_card_run_id

{% docs column_pinval_assessment_card_run_id %}
Run ID for the model run associated with this card and its values.

For PINs that are in the assessment triad for a given assessment year,
this column will correspond to the final model run ID for the PIN's township
in that assessment year. For PINs that are _not_ in the assessment triad, this
column will be null.
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

## combined_bldg_sf

{% docs column_pinval_combined_bldg_sf %}
Combined building SF for all cards in the PIN.

This field is only computed for recent small multicards, which are
distinguished by the value `is_parcel_small_multicard == TRUE`. It
will be null for all other cards.
{% enddocs %}

## comps_assessment_year

{% docs column_pinval_comps_assessment_year %}
Assessment year for the model run associated with this card and its comps
{% enddocs %}

## comps_run_id

{% docs column_pinval_comps_run_id %}
Run ID for the model run associated with this card and its comps.

Our data tests guarantee that there is only ever one comps run per assessment
year, so for uniqueness purposes, this column should be interchangeable with
`assessment_year`.
{% enddocs %}

## is_frankencard

{% docs column_pinval_is_frankencard %}
Whether this card is the "frankencard" for its PIN.

"Frankencards" refer to the largest card by building square footage in a
multicard PIN, or the largest card with the lowest card number in the case of
square footage ties. We use the PIN's frankencard as the basis for its non-SF
characteristics when valuing small (2-3 card) multicards.

Note that this valuation method is new as of 2025 and restricted to 2-3 card
PINs, so it will only ever be true if `assessment_year` and
`meta_pin_num_cards` meet those conditions.
{% enddocs %}

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
