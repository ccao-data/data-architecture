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

**Primary Key**: `year`, `pin`, `card`
{% enddocs %}

# vw_pin_address

{% docs view_vw_pin_address %}
Source of truth view for PIN address, both legal (property address)
and mailing (owner/taxpayer address).

### Nuance

- Mailing addresses and owner names have not been regularly updated since 2017.
- Newer properties may be missing a mailing or property address, as they
  need to be assigned one by the postal service.

**Primary Key**: `year`, `pin`
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

- Only contains appeal decisions for the Assessor's Office. Board of Review
  appeal decisions can be found on the
  [Cook County Open Data portal here](https://datacatalog.cookcountyil.gov/Property-Taxation/Board-of-Review-Appeal-Decision-History/7pny-nedm).
- This view is _not_ unique by PIN and year, as a single PIN can have an
  appeal and CofE/omitted assessment in a given year.

**Primary Key**: `year`, `pin`, `case_no`
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

# vw_pin_exempt

{% docs view_vw_pin_exempt %}
Parcels with property tax-exempt status across all of Cook County per tax year,
from 2022 on. Exempt parcels are typically owned by non-profits, religious
institutions, or local governments.

### Nuance

- Mailing addresses and owner names have not been regularly updated since 2017.
- Newer properties may be missing a mailing or property address, as they
  need to be assigned one by the postal service.
{% enddocs %}

# vw_pin_history

{% docs view_vw_pin_history %}
Current and prior years' assessments by PIN in wide format.

Assessed values are only populated once townships are "closed" and their
corresponding `procname` value is updated in `iasworld.asmt_all`.

**Primary Key**: `year`, `pin`
{% enddocs %}

# vw_pin_land

{% docs view_vw_pin_land %}
View containing aggregate land square footage for all PINs.

### Nuance

- Different sections of land on the same PIN can be valued at different
  rates, which this view does not capture.

**Primary Key**: `year`, `pin`
{% enddocs %}

# vw_pin_permit

{% docs view_vw_pin_permit %}
View containing building permits organized by PIN, with extra metadata
recorded by CCAO permit specialists during the permit processing workflow.

**Primary Key**: `pin`, `date_issued`
{% enddocs %}

# vw_pin_sale

{% docs view_vw_pin_sale %}
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

# vw_pin_status

{% docs view_vw_pin_status %}
Collection of various different PIN-level physical and assessment-related
statuses collected and documented across the CCAO and Data Department.
Constructs the Data Department's AHSAP indicator.

### Nuance

- Parcels can have different CDUs from multiple tables. See PIN 05272010320000.

**Primary Key**: `year`, `pin`
{% enddocs %}

# vw_pin_universe

{% docs view_vw_pin_universe %}
PIN-level geographic location and spatially joined locations.

If you want to know where a PIN is or what boundaries it lies within, this
is the view you're looking for.

### Nuance

- `spatial.parcel` typically lags behind `iasworld.pardat` by a year, so data
  for current year can be relatively sparse or missing. Parcel shapefiles
  typically become available to populate this view at the end of each year.
- `spatial.township` is not yearly.

**Primary Key**: `year`, `pin`
{% enddocs %}

# vw_pin_value

{% docs view_vw_pin_value %}
Assessed and market values by PIN and year, for each assessment stage.

The assessment stages are:

1. `mailed` - Values initially mailed by the Assessor
2. `certified` - Values after the Assessor has finished processing appeals
2. `board` - Values after the Board of Review has finished their appeals

### Assumptions

- Taking the max value by 14-digit PIN and year is sufficient for accurate
  values. We do this because even given the criteria to de-dupe `asmt_all`,
  we still end up with duplicates by PIN and year.
- Market value (`_mv`) columns accurately reflect incentives, statute,
  levels of assessment, building splits, etc.

### Nuance

- Market values only exist for stages after and including `2020 Board`. Prior
  to that, market values were stored/tracked in the county mainframe and are
  not easily retrievable.

**Primary Key**: `year`, `pin`
{% enddocs %}
