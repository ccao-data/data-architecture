{% docs vw_pin_history_test %}
Current and prior years' assessments by PIN in wide format.

## Nuance

`spatial.township` is not yearly.
{% enddocs %}

{% docs vw_pin_value_test %}
CCAO mailed total, CCAO final, and BOR final values for each PIN by year.

## Assumptions
Taking the max value by 14-digit pin and year is sufficient for accurate values.
We do this because even given the criteria to de-dupe `asmt_all`, we still end
up with duplicates by pin and year.
{% enddocs %}

{% docs vw_pin_appeal_test %}
Appeals by PIN.

## Assumptions

* Appeal types coding is static
* Status coding is static
* Condo/coop appeal type is new, so we have to assume which classes fit the category prior to 2021
{% enddocs %}
