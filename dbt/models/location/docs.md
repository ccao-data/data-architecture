# access

{% docs table_access %}
Parcel-level accessibility metrics such as walkability scores from CMAP.

**Primary Key**: `pin10`, `year`
{% enddocs %}

# census

{% docs table_census %}
Decennial Census geographies (tracts, block groups, etc.) intersected with
parcel centroids.

**Primary Key**: `pin10`, `year`
{% enddocs %}

# census_2020

{% docs table_census_2020 %}
Decennial Census geographies (tracts, block groups, etc.) intersected with
parcel centroids from *all* years, not just those that existed in 2020. In
actuality, we use 2022 since geoids didn't update until then (currently applies
just to IHS Housing Index data).

**Primary Key**: `pin10`, `year`
{% enddocs %}

# census_acs5

{% docs table_census_acs5 %}
American Community Survey (ACS) Census geographies
(tracts, block groups, etc.) intersected with parcel centroids.

**Primary Key**: `pin10`, `year`
{% enddocs %}

# chicago

{% docs table_chicago %}
Chicago-specific geographies such as community areas and
police districts, intersected with parcel centroids.

**Primary Key**: `pin10`, `year`
{% enddocs %}

# crosswalk_year_fill

{% docs table_crosswalk_year_fill %}
Table used in `_fill` views to fill joined geographies forward (or backward)
in time.

This table (and its filling) are necessary because not all geographic data
is released every year. For example, an ACS5 release from 2019 would be
filled forward into future years (until the next release).

**Primary Key**: `pin10`, `year`
{% enddocs %}

# economy

{% docs table_economy %}
Economic and incentive geographies such as enterprise zones and
industrial growth zones, intersected with parcel centroids.

**Primary Key**: `pin10`, `year`
{% enddocs %}

# environment

{% docs table_environment %}
Environmental factors such as flood risk, intersected with parcel centroids.

**Primary Key**: `pin10`, `year`
{% enddocs %}

# other

{% docs table_other %}
Miscellaneous geographies, intersected with parcel centroids.

**Primary Key**: `pin10`, `year`
{% enddocs %}

# political

{% docs table_political %}
Political boundaries and districts such as Chicago ward, judicial districts,
etc., intersected with parcel centroids.

**Primary Key**: `pin10`, `year`
{% enddocs %}

# school

{% docs table_school %}
School district boundaries (elementary, secondary, and unified), intersected
with parcel centroids.

Note that these boundaries are sourced from Cook County GIS. There are four
total versions of the same school district boundaries:

1. Spatial boundaries (polygons) from Cook County GIS (this table)
2. Spatial boundaries from the Census/Tigris (`location.census`)
3. Spatial taxing boundaries from Cook County GIS (`location.tax`)
4. PIN-level school district ID pulled directly from each PIN's
   tax bill (`tax.agency`)

**Primary Key**: `pin10`, `year`
{% enddocs %}

# tax

{% docs table_tax %}
Taxing district boundaries such as libraries, municipalities, and
park districts, intersected with parcel centroids.

**Primary Key**: `pin10`, `year`
{% enddocs %}

# vw_pin10_location

{% docs view_vw_pin10_location %}
View combining all other `location.*` tables. Adds column-level descriptions
to every variable.

This is essentially the entire universe of Data Department geographic
data attached to each parcel.

**Primary Key**: `pin10`, `year`
{% enddocs %}

# vw_pin10_location_fill

{% docs view_vw_pin10_location_fill %}
Identical to `location.vw_pin10_location`, but forward and backward filled
according to `location.crosswalk_year_fill`.

**Primary Key**: `pin10`, `year`
{% enddocs %}
