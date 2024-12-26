# vw_pin_universe

{% docs view_vw_pin_universe %}
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