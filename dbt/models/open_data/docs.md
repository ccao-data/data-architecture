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