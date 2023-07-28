{% docs vw_pin_history_test %}
Current and prior years' assessments by PIN in wide format.

### Nuance

`spatial.township` is not yearly.
{% enddocs %}

{% docs vw_pin_value_test %}
CCAO mailed total, CCAO final, and BOR final values for each PIN by year.

### Assumptions
Taking the max value by 14-digit pin and year is sufficient for accurate values.
We do this because even given the criteria to de-dupe `asmt_all`, we still end
up with duplicates by pin and year.
{% enddocs %}

{% docs vw_pin_appeal_test %}
Appeals by PIN.

### Assumptions

* Appeal types coding is static
* Status coding is static
* Condo/coop appeal type is new, so we have to assume which classes fit the
  category prior to 2021
{% enddocs %}

{% docs vw_card_res_char_test %}
View that standardizes residential property characteristics for use in modeling
and reporting.

### Assumptions
`char_renovation` (`dweldat.user3`) is updated when HI permits are filed.

### Nuance
Land is parcel-level.
{% enddocs %}

{% docs vw_pin_address_test %}
Source of truth view for PIN address, both legal and mailing.

### Assumptions
Mailing addresses are updated when properties transfer ownership.
{% enddocs %}

{% docs vw_pin_condo_char_test %}
View containing cleaned, filled data for condo modeling. Missing data is
filled as follows:

Condo characteristics are filled with whatever the most recent non-`NULL`
value is. This assumes that new condo data is more accurate than older
data, not that it represents a change in a unit's characteristics. This
should only be the case while condo characteristics are pulled from Excel
workbooks rather than iasWorld.

### Assumptions

* `cur = 'Y'` or a single row for a parcel even when `cur = 'N'` indicate rows
that should be included from `oby` and `comdat`
* Null proration rate for condo unit indicates condo isn't associated with other
units
* Proration rates in `oby` and `comdat` are parcel-level
* `effyr` is equivalent to `yrblt` when `yrblt` is `NULL`
* Most recent value for CDU is most relevant if it has been re-coded to `NULL`

### Nuance
* Land is parcel-level
* 299s can have there class coded as 2-99 in iasWorld
* Condo parcels can exist in `pardat` but not `comdat` (this is probably a
reclassification issue)
{% enddocs %}

{% docs vw_pin_sale_test %}
View containing unique, filtered sales.

### Assumptions
* `iasworld.sales.deactivat` properly indicates sales that should and shouldn't
  be included
* For sales not unique by pin and sale date, the most expensive sale for a
  given day/pin is used
* Some parcels are sold for the exact same price soon after an initial sale -
  we ignore duplicate prices for pins if they've sold in the last 12 months

### Nuance
* `iasworld.sales.nopar` is inaccurate: excludes quit claims, executor deeds,
  beneficial interests
* `mydec` data is given precedence over `iasworld.sales` prior to 2021
* Multicard sales are excluded from `mydec` data because they can't be joined
  to `iasworld.sales` (which is only parcel-level) without creating duplicates
{% enddocs %}

{% docs vw_pin_universe_test %}
Source of truth view for PIN location.

### Nuance
* `spatial.parcel` typically lags behind `iasworld.pardat` by a year, so data
  for current year can be relatively sparse
* `spatial.township` is not yearly
{% enddocs %}
