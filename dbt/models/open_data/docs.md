# vw_appeal

{% docs view_vw_appeal %}
View of assessment appeals by stage (wide format). Shows appeal decision,
reason, and results.

### Assumptions

- Appeal types coding is static.
- Status coding is static.
- Condo/co-op appeal type is new, so we have to assume which classes fit the
  category prior to 2021.

### Nuance

- Only contains appeal decisions for the Assessor's Office. Board of Review
  appeal decisions can be found on the
  [Cook County Open Data portal here](https://datacatalog.cookcountyil.gov/Property-Taxation/Board-of-Review-Appeal-Decision-History/7pny-nedm).
- This view is _not_ unique by PIN and year, as a single PIN can have an
  appeal and CofE/omitted assessment in a given year.

**Primary Key**: `year`, `pin`, `case_no`
{% enddocs %}

# vw_assessed_value

{% docs view_vw_assessed_value %}
Current and prior years' assessments by PIN in wide format.

Assessed values are only populated once townships are "closed" and their
corresponding `procname` value is updated in `iasworld.asmt_all`.

**Primary Key**: `year`, `pin`
{% enddocs %}

# vw_parcel_address

{% docs view_vw_parcel_address %}
Source of truth view for PIN address, both legal (property address)
and mailing (owner/taxpayer address).

### Nuance

- Mailing addresses and owner names have not been regularly updated since 2017.
- Newer properties may be missing a mailing or property address, as they
  need to be assigned one by the postal service.

**Primary Key**: `year`, `pin`
{% enddocs %}

# vw_parcel_sale

{% docs view_vw_parcel_sale %}
View containing cleaned and deduplicated PIN-level sales.

Sourced from `iasworld.sales`, which is sourced from
[MyDec](https://mytax.illinois.gov/MyDec/_/). See below for lineage details.

### Assumptions

- `deactivat` properly indicates sales that should and shouldn't be included.
- For sales not unique by pin and sale date, the most expensive sale for a
  given day/PIN is used.
- Some parcels are sold for the exact same price soon after an initial sale -
  we ignore duplicate prices for PINs if they've sold in the last 12 months.

### Nuance

- `nopar` is inaccurate: it excludes quit claims, executor deeds,
  and beneficial interests.
- `sale.mydec` data is given precedence over `iasworld.sales` prior to 2021
- Multicard sales are excluded from `mydec` data because they can't be joined
  to `iasworld.sales` (which is only parcel-level) without creating duplicates
- Sales are unique by `doc_no` if multisales are excluded. When multisales are
  _not_ excluded, sales are unique by `doc_no` and `pin`.
- We include iasworld sales and mydec sales only if the mydec sale isn't already
  present in iasworld (calculated by doc_no). This allows us to use mydec sales
  for analysis or modeling if the iasworld sales ingest is lags behind mydec.

### Lineage

This view is constructed from [MyDec](https://mytax.illinois.gov/MyDec/_/) data
gathered and filtered by numerous parties. It uses the `iasworld.sales` table
as a base, which is itself constructed from two separate sources of MyDec data.
Current MyDec records are ingested into `iasworld.sales` using a manual import
process. The full data lineage looks something like:

![Data Flow Diagram](./assets/sales-lineage.svg)

**Primary Key**: `doc_no`, `pin`
{% enddocs %}

# vw_parcel_universe_current

{% docs view_vw_parcel_universe_current %}
PIN-level geographic location and spatially joined locations. Limited to most
recent year only. Mirrors `default.vw_pin_universe`.

If you want to know where a PIN is or what boundaries it lies within, this
is the view you're looking for.

### Nuance

- `spatial.parcel` typically lags behind `iasworld.pardat` by a year, so data
  for current year can be relatively sparse or missing. Parcel shapefiles
  typically become available to populate this view at the end of each year.
- `spatial.township` is not yearly.

**Primary Key**: `year`, `pin`
{% enddocs %}

# vw_parcel_universe_historic

{% docs view_vw_parcel_universe_historic %}
PIN-level geographic location and spatially joined locations. Mirrors `default.vw_pin_universe`.

If you want to know where a PIN is or what boundaries it lies within, this
is the view you're looking for.

### Nuance

- `spatial.parcel` typically lags behind `iasworld.pardat` by a year, so data
  for current year can be relatively sparse or missing. Parcel shapefiles
  typically become available to populate this view at the end of each year.
- `spatial.township` is not yearly.

**Primary Key**: `year`, `pin`
{% enddocs %}

# vw_res_condo_unit_char

{% docs view_vw_res_condo_unit_char %}
View containing cleaned, filled data for condo modeling. Missing data is
filled as follows:

Condo characteristics are filled with whatever the most recent non-`NULL`
value is. This assumes that new condo data is more accurate than older
data, not that it represents a change in a unit's characteristics. This
should only be the case while condo characteristics are pulled from Excel
workbooks rather than iasWorld.

### Assumptions

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

**Primary Key**: `year`, `pin`
{% enddocs %}

# vw_sf_mf_improvement_char

{% docs view_vw_sf_mf_improvement_char %}
View to standardize residential property characteristics for use in modeling
and reporting.

### Nuance

- Only contains characteristics for "regression class" residential properties,
  see `ccao.class_dict` for details.
- Observations are card-level, i.e. each row is one building. Note that a
  card does not necessarily equal a PIN.
- Land is parcel-level, not card-level.

**Primary Key**: `year`, `pin`, `card`
{% enddocs %}