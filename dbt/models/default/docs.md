# vw_card_res_char

{% docs view_vw_card_res_char %}
View to standardize residential property characteristics for use in modeling
and reporting.

### Nuance

- Only contains characteristics for "regression class" residential properties,
  see `ccao.class_dict` for details.
- Observations are card-level, i.e. each row is one building. Note that a
  card does not necessarily equal a PIN.
- Land is parcel-level, not card-level.
{% enddocs %}

# vw_pin_address

{% docs view_vw_pin_address %}
Source of truth view for PIN address, both legal (property address)
and mailing (owner/taxpayer address).

### Assumptions

- Mailing addresses are updated when properties transfer ownership.

### Nuance

- Newer properties may be missing a mailing or property address, as they
  need to be assigned one by the postal service.
{% enddocs %}

# vw_pin_appeal

{% docs view_vw_pin_appeal %}
View of assessment appeals by stage (wide format). Shows appeal decision,
reason, and results.

### Assumptions

- Appeal types coding is static.
- Status coding is static.
- Condo/co-op appeal type is new, so we have to assume which classes fit the
  category prior to 2021.

### Nuance

- Not live updated. This table is only updated as townships are "closed"
  according to the Assessor's town schedule.
- Only contains appeal decisions for the Assessor's Office. Board of Review
  appeal decisions can be found on the
  [Cook County Open Data portal here](https://datacatalog.cookcountyil.gov/Property-Taxation/Board-of-Review-Appeal-Decision-History/7pny-nedm).
{% enddocs %}

# vw_pin_condo_char

{% docs view_vw_pin_condo_char %}
View containing cleaned, filled data for condo modeling. Missing data is
filled as follows:

Condo characteristics are filled with whatever the most recent non-`NULL`
value is. This assumes that new condo data is more accurate than older
data, not that it represents a change in a unit's characteristics. This
should only be the case while condo characteristics are pulled from Excel
workbooks rather than iasWorld.

### Assumptions

- `cur = 'Y'` or a single row for a parcel even when `cur = 'N'` indicate rows
  that should be included from `oby` and `comdat`.
- A null proration rate for condo unit indicates the condo isn't associated
  with other units.
- Proration rates in `oby` and `comdat` are parcel-level.
- `effyr` is equivalent to `yrblt` when `yrblt` is `NULL`.
- The most recent value for CDU is most relevant if it has
  been re-coded to `NULL`

### Nuance

- Land is parcel-level.
- Condo parcels can exist in `pardat` but not `comdat` (this is probably a
  reclassification issue).
{% enddocs %}

# vw_pin_exempt

{% docs view_vw_pin_exempt %}
Parcels with property tax-exempt status across all of Cook County per tax year,
from 2022 on. Exempt parcels are typically owned by non-profits, religious
institutions, or local governments.

### Nuance

- Newer properties may be missing a mailing or property address, as they
  need to be assigned one by the postal service.
{% enddocs %}

# vw_pin_history

{% docs view_vw_pin_history %}
Current and prior years' assessments by PIN in wide format.
{% enddocs %}

# vw_pin_land

{% docs view_vw_pin_land %}
View containing aggregated land square footage.
{% enddocs %}

# vw_pin_sale

{% docs view_vw_pin_sale %}
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

# vw_pin_universe

{% docs view_vw_pin_universe %}
Source of truth view for PIN location.

### Nuance
* `spatial.parcel` typically lags behind `iasworld.pardat` by a year, so data
  for current year can be relatively sparse
* `spatial.township` is not yearly
{% enddocs %}

# vw_pin_value

{% docs view_vw_pin_value %}
CCAO mailed total, CCAO final, and BOR final values for each PIN by year.

### Assumptions
Taking the max value by 14-digit pin and year is sufficient for accurate values.
We do this because even given the criteria to de-dupe `asmt_all`, we still end
up with duplicates by pin and year.
{% enddocs %}
