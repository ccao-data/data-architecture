# cnt_pin_num_bus_stop

{% docs table_cnt_pin_num_bus_stop %}
Count of number of bus stops within half mile of each PIN.

Includes PACE and CTA bus stops.

**Primary Key**: `pin10`, `year`
{% enddocs %}

# cnt_pin_num_foreclosure

{% docs table_cnt_pin_num_foreclosure %}
Count of number of foreclosures within a half mile of each PIN.

**Primary Key**: `pin10`, `year`
{% enddocs %}

# cnt_pin_num_school

{% docs table_cnt_pin_num_school %}
Count of number of schools (any kind) within a half mile of each PIN.

**Primary Key**: `pin10`, `year`
{% enddocs %}

# crosswalk_year_fill

See `location/docs.md`.

# dist_pin_to_bike_trail

{% docs table_dist_pin_to_bike_trail %}
Distance from each PIN to the nearest bike trail.

**Primary Key**: `pin10`, `year`
{% enddocs %}

# dist_pin_to_cemetery

{% docs table_dist_pin_to_cemetery %}
Distance from each PIN to the nearest cemetery.

**Primary Key**: `pin10`, `year`
{% enddocs %}

# dist_pin_to_cta_route

{% docs table_dist_pin_to_cta_route %}
Distance from each PIN to the nearest CTA route.

In this case, "route" is inclusive of any CTA train tracks. So living
directly next to the CTA tracks would have a low distance, even if you
are not near an actual CTA stop.

**Primary Key**: `pin10`, `year`
{% enddocs %}

# dist_pin_to_cta_stop

{% docs table_dist_pin_to_cta_stop %}
Distance from each PIN to the nearest CTA stop.

In this case, "stop" means any CTA train station, _not_ bus stops.

**Primary Key**: `pin10`, `year`
{% enddocs %}

# dist_pin_to_golf_course

{% docs table_dist_pin_to_golf_course %}
Distance from each PIN to the nearest golf course.

**Primary Key**: `pin10`, `year`
{% enddocs %}

# dist_pin_to_hospital

{% docs table_dist_pin_to_hospital %}
Distance from each PIN to the nearest hospital.

**Primary Key**: `pin10`, `year`
{% enddocs %}

# dist_pin_to_lake_michigan

{% docs table_dist_pin_to_lake_michigan %}
Distance from each PIN to the Lake Michigan shoreline.

**Primary Key**: `pin10`, `year`
{% enddocs %}

# dist_pin_to_major_road

{% docs table_dist_pin_to_major_road %}
Distance from each PIN to the nearest major road.

**Primary Key**: `pin10`, `year`
{% enddocs %}

# dist_pin_to_metra_route

{% docs table_dist_pin_to_metra_route %}
Distance from each PIN to the nearest Metra route.

In this case, "route" is inclusive of any Metra train tracks. So living
directly next to the Metra tracks would have a low distance, even if you
are not near an actual Metra stop.

**Primary Key**: `pin10`, `year`
{% enddocs %}

# dist_pin_to_metra_stop

{% docs table_dist_pin_to_metra_stop %}
Distance from each PIN to the nearest Metra stop.

**Primary Key**: `pin10`, `year`
{% enddocs %}

# dist_pin_to_park

{% docs table_dist_pin_to_park %}
Distance from each PIN to the nearest park.

**Primary Key**: `pin10`, `year`
{% enddocs %}

# dist_pin_to_pin

{% docs table_dist_pin_to_pin %}
View to find the three nearest neighbor PINs for every PIN for every year.

**Primary Key**: `pin10`, `year`
{% enddocs %}

# dist_pin_to_pin_intermediate

{% docs table_dist_pin_to_pin_intermediate %}
Intermediate table used to generate `proximity.dist_pin_to_pin`.

The `proximity.dist_pin_to_pin` view is intended to record distances to the
three closest PINs for all PINs in the county for all years in the data.
This type of recursive spatial query is expensive, however, and some PINs are
quite far (>1km) from the nearest three PINs, so we use intermediate tables
to strike a balance between data completeness and computational efficiency.

To compute the full set of distances in `proximity.dist_pin_to_pin`, we first
generate PIN-to-PIN distances using a 1km buffer and store the results in the
`proximity.dist_pin_to_pin_1km` table. Then, we query for PINs that did not have
any matches within 1km and redo the distance query with an expanded 10km buffer,
storing the results in the `proximity.dist_pin_to_pin_10km` table. Finally, the
union of the 1km table and the 10km table is aliased to the
`proximity.dist_pin_to_pin` view for ease of querying.

**Primary Key**: `pin10`, `year`
{% enddocs %}

# dist_pin_to_railroad

{% docs table_dist_pin_to_railroad %}
Distance from each PIN to the nearest rail track of any kind.

**Primary Key**: `pin10`, `year`
{% enddocs %}

# dist_pin_to_water

{% docs table_dist_pin_to_water %}
Distance from each PIN to the nearest water of any kind.

**Primary Key**: `pin10`, `year`
{% enddocs %}

# vw_pin10_proximity

{% docs view_vw_pin10_proximity %}
View combining all other `proximity.*` tables. Adds column-level descriptions
to every variable.

This is essentially the entire universe of Data Department proximity metrics
(distance to amenities, counts of nearby things, etc.).

**Primary Key**: `pin10`, `year`
{% enddocs %}

# vw_pin10_proximity_fill

{% docs view_vw_pin10_proximity_fill %}
Identical to `location.vw_pin10_proximity`, but forward and backward filled
according to `proximity.crosswalk_year_fill`.

**Primary Key**: `pin10`, `year`
{% enddocs %}
