## is_report_eligible

{% docs column_pinval_is_report_eligible %}
When `TRUE`, this PIN is eligible for a PINVAL report for the given model run
{% enddocs %}

## meta_card_num

{% docs meta_card_num %}
The card number for the card.

There are two cases in which this column might be null:

- **No entry exists for this card in `model.assessment_card`**. This
  means that the card was ineligible for a model value and got filtered out of
  the model's assessment set prior to prediction. In this case, the `meta_pin`
  column (and all other columns that come from `model.assessment_card`) will
  also be null.
- **The PIN does not have any cards.** This indicates a data error that
  causes the model to ignore the parcel for valuation purposes. In this case,
  the `reason_report_ineligible` column will have the value `'missing_card'`.

{% enddocs %}

## model_run_id

{% docs column_pinval_model_run_id %}
Run ID for the model run associated with this card and its values.

Prefer this to `run_id`, which comes from `model.assessment_card`,
because `run_id` will be null if the parcel is ineligible for a report
for this model run. In contrast, this column will never be null.
{% enddocs %}

## parcel_class

{% docs column_pinval_parcel_class %}
The class for the parcel that this card is associated with.

This field is different from `char_class`, which comes from
`model.assessment_card` and represents the card class. Card classes do not
necessarily match the class of the parcel that the card is associated with.
This field will also always be present even if `char_class` is null, because
this field comes from `default.vw_pin_universe` which contains PINs that
are not present in the assessment set due to not being a residential
regression class.
{% enddocs %}

## parcel_class_description

{% docs column_pinval_parcel_class_description %}
The short description for the card's parcel class.

See `parcel_class` for details on the difference between parcel classes and
card classes in the context of this view.
{% enddocs %}

## pin

{% docs column_pinval_pin %}
The card's parcel identification number (PIN).

In general, you should prefer this column to `meta_pin` when querying from this
table, since `meta_pin` comes from `model.assessment_card` and will be null for
PINs that the res model does not value. You can safely use `meta_pin` when
filtering by `is_report_eligible = TRUE`, however.
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
