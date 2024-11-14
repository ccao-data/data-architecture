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

# dist_pin_to_airport

{% docs table_dist_pin_to_airport %}
Distance from each PIN to O'Hare airport, and to Midway airport, in feet. Also
includes estimated DNL (noise) contribution from each airport, and predicted DNL
as a result of contributions from both airports plus baseline DNL of 50.

**Primary Key**: `pin10`, `year`
{% enddocs %}

# dist_pin_to_arterial_road

{% docs table_dist_pin_to_arterial_road %}
Distance from each PIN to the nearest arterial road. Data is derived from Illinois Department of Transportation
	Added features include 
	- lanes
	- average daily traffic
	- speed limit
	- road surface

**Primary Key**: `pin10`, `year`
{% enddocs %}

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

# dist_pin_to_collector_road

{% docs table_dist_pin_to_collector_road %}
Distance from each PIN to the nearest collector road. Data is derived from Illinois Department of Transportation
	Added features include 
	- lanes
	- average daily traffic
	- speed limit
	- road surface

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

# dist_pin_to_grocery_store

{% docs table_dist_pin_to_grocery_store %}
Distance from each PIN to the nearest grocery store. Locations sourced from OpenStreetMap (OSM).
	
	OSM tags include:
	
	- `shop=supermarket`
	- `shop=wholesale`
	- `shop=greengrocer`
	  
	Only attributes with valid names are kept.

**Primary Key**: `pin10`, `year`
{% enddocs %}

# dist_pin_to_highway_road

{% docs table_dist_pin_to_highway_road %}
Distance from each PIN to the nearest highway. Data is derived from Illinois Department of Transportation
	Added features include 
	- lanes
	- average daily traffic
	- speed limit
	- road surface

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

# dist_pin_to_new_construction

{% docs table_dist_pin_to_new_construction %}
Distance from each PIN to the nearest new construction.
New construction is defined as being within the past 3 years.

**Primary Key**: `pin10`, `year`
{% enddocs %}

# dist_pin_to_park

{% docs table_dist_pin_to_park %}
Distance from each PIN to the nearest park.

**Primary Key**: `pin10`, `year`
{% enddocs %}

# dist_pin_to_pin

{% docs view_dist_pin_to_pin %}
View to find the three nearest neighbor PINs for every PIN for every year.

**Primary Key**: `pin10`, `year`
{% enddocs %}

# dist_pin_to_pin_intermediate

{% docs table_dist_pin_to_pin_intermediate %}
Intermediate table used to generate `proximity.dist_pin_to_pin`.

The `proximity.dist_pin_to_pin` view is intended to record distances to the
three closest PINs for all PINs in the county for all years in the data.
This type of recursive spatial query is expensive, however, and some PINs are
quite far (>1mi) from the nearest three PINs, so we use intermediate tables
to strike a balance between data completeness and computational efficiency.

To compute the full set of distances in `proximity.dist_pin_to_pin`, we first
generate PIN-to-PIN distances using a small buffer and store the results in the
`proximity.dist_pin_to_pin_01` table. Then, we query for PINs that did not have
any matches within the buffer and redo the distance query with a slightly larger
buffer, storing the results in the `proximity.dist_pin_to_pin_02` table. The
process repeats until neighbors are found for all PINs.

Finally, the union of all the intermediate tables is aliased to the
`proximity.dist_pin_to_pin` view for ease of querying.

**Primary Key**: `pin10`, `year`
{% enddocs %}

# dist_pin_to_railroad

{% docs table_dist_pin_to_railroad %}
Distance from each PIN to the nearest rail track of any kind.

**Primary Key**: `pin10`, `year`
{% enddocs %}

# dist_pin_to_secondary_road

{% docs table_dist_pin_to_secondary_road %}
Distance from each PIN to the nearest secondary road.

**Primary Key**: `pin10`, `year`
{% enddocs %}

# dist_pin_to_stadium

{% docs table_dist_pin_to_stadium %}
Distance from each PIN to the nearest sports stadium.

**Primary Key**: `pin10`, `year`
{% enddocs %}

# dist_pin_to_traffic_daily_traffic

{% docs table_dist_pin_to_traffic_daily_traffic %}
Distance from each PIN to the valid value of daily traffic.

**Primary Key**: `pin10`, `year`
{% enddocs %}

# dist_pin_to_university

{% docs table_dist_pin_to_university %}
Distance from each PIN to the nearest university.

**Primary Key**: `pin10`, `year`
{% enddocs %}

# dist_pin_to_vacant_land

{% docs table_dist_pin_to_vacant_land %}
Distance from each PIN to the nearest vacant land (class 100) parcel
larger than 1,000 square feet.

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
