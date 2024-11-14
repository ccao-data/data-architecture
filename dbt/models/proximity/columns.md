## airport_data_year

{% docs column_airport_data_year %}
Set to year 2019 for all PINs. Note that O'Hare noise predictions
are built from O'Hare Modernization Program (OMP) projected values.
{% enddocs %}

## airport_dnl_midway

{% docs column_airport_dnl_midway %}
Estimated contribution of Midway airport to PIN noise.

Measured in decibels; to be interpreted as contribution to DNL
(Day-Night Level) estimate of 24-hour average of decibel level.
Produced by modeling noise level as inverse-square falloff
from a point source at the centroid of the airport.
{% enddocs %}

## airport_dnl_ohare

{% docs column_airport_dnl_ohare %}
Estimated contribution of O'Hare airport to PIN noise.

Measured in decibels; to be interpreted as contribution to DNL
(Day-Night Level) estimate of 24-hour average of decibel level.
Produced by modeling noise level as inverse-square falloff
from a point source at the centroid of the airport.
{% enddocs %}

## airport_dnl_total

{% docs column_airport_dnl_total %}
Estimated DNL for a PIN, assuming a baseline DNL of 50 ("quiet suburban") and
adding predicted noise from O'Hare and Midway airports to that baseline.

For more on DNL, see
<https://www.faa.gov/regulations_policies/policy_guidance/noise/basics>
{% enddocs %}

## airport_midway_dist_ft

{% docs column_airport_midway_dist_ft %}
Distance to centroid of Midway airport (feet)
{% enddocs %}

## airport_ohare_dist_ft

{% docs column_airport_ohare_dist_ft %}
Distance to centroid of O'Hare airport (feet)
{% enddocs %}

## nearest_arterial_road_name

{% docs column_nearest_arterial_road_name %}
Nearest arterial road name.

Road data sourced from Illinois Department of Transportation.
{% enddocs %}

## nearest_arterial_road_dist_ft

{% docs column_nearest_arterial_road_dist_ft %}
Distance to nearest arterial road.

Road data sourced from Illinois Department of Transportation.
{% enddocs %}

## nearest_arterial_road_daily_traffic

{% docs column_nearest_arterial_road_daily_traffic %}
Daily traffic of nearest arterial road.

Road data sourced from Illinois Department of Transportation.
{% enddocs %}

## nearest_arterial_road_lanes

{% docs column_nearest_arterial_road_daily_traffic %}
Number of lanes for the nearest arterial road.

Road data sourced from Illinois Department of Transportation.
{% enddocs %}

## nearest_arterial_road_surface type

{% docs column_nearest_arterial_road_daily_traffic %}
Surface type for the nearest arterial road (for example brick, stone, etc.).

Road data sourced from Illinois Department of Transportation.
{% enddocs %}

## nearest_arterial_road_speed_limit

{% docs column_nearest_arterial_road_daily_traffic %}
Speed limit for the nearest arterial road

Road data sourced from Illinois Department of Transportation
{% enddocs %}

## avg_school_rating_in_half_mile

{% docs column_avg_school_rating_in_half_mile %}
Average school rating of schools within half mile.

Schools of any type (elementary, secondary, etc.) are included.
School ratings sourced from [GreatSchools](https://www.greatschools.org/)
{% enddocs %}

## lake_michigan_dist_ft

{% docs column_lake_michigan_dist_ft %}
Distance to Lake Michigan shoreline (feet).

Shoreline sourced from Census hydrography files
{% enddocs %}

## nearest_bike_trail_dist_ft

{% docs column_nearest_bike_trail_dist_ft %}
Nearest bike trail distance (feet).

Bike trail data sourced from Cook County GIS
{% enddocs %}

## nearest_bike_trail_id

{% docs column_nearest_bike_trail_id %}
Nearest bike trail Cook County ID.

Bike trail data sourced from Cook County GIS
{% enddocs %}

## nearest_bike_trail_name

{% docs column_nearest_bike_trail_name %}
Nearest bike trail name.

Bike trail data sourced from Cook County GIS
{% enddocs %}

## nearest_cemetery_dist_ft

{% docs column_nearest_cemetery_dist_ft %}
Nearest cemetery distance (feet).

Cemetery data sourced from Cook County GIS
{% enddocs %}

## nearest_cemetery_gnis_code

{% docs column_nearest_cemetery_gnis_code %}
Nearest cemetery GNIS code.

Cemetery data sourced from Cook County GIS
{% enddocs %}

## nearest_cemetery_name

{% docs column_nearest_cemetery_name %}
Nearest cemetery name.

Cemetery data sourced from Cook County GIS
{% enddocs %}

## nearest_collector_road_name

{% docs column_nearest_collector_road_name %}
Nearest collector road name.

Road data sourced from Illinois Department of Transportation.
{% enddocs %}

## nearest_collector_road_dist_ft

{% docs column_nearest_collector_road_dist_ft %}
Distance to nearest collector road.

Road data sourced from Illinois Department of Transportation.
{% enddocs %}

## nearest_collector_road_daily_traffic

{% docs column_nearest_collector_road_daily_traffic %}
Daily traffic of nearest collector road.

Road data sourced from Illinois Department of Transportation.
{% enddocs %}

## nearest_collector_road_lanes

{% docs column_nearest_collector_road_daily_traffic %}
Number of lanes for the nearest collector road.

Road data sourced from Illinois Department of Transportation.
{% enddocs %}

## nearest_collector_road_surface type

{% docs column_nearest_collector_road_daily_traffic %}
Surface type for the nearest collector road (for example brick, stone, etc.).

Road data sourced from Illinois Department of Transportation.
{% enddocs %}

## nearest_collector_road_speed_limit

{% docs column_nearest_collector_road_daily_traffic %}
Speed limit for the nearest collector road

Road data sourced from Illinois Department of Transportation
{% enddocs %}
## nearest_cta_route_dist_ft

{% docs column_nearest_cta_route_dist_ft %}
Nearest CTA route distance (feet).

Routes include any active CTA tracks. Route data sourced from CTA GTFS feeds
{% enddocs %}

## nearest_cta_route_id

{% docs column_nearest_cta_route_id %}
Nearest CTA route short name (`Red`, `G`, etc.).

Routes include any active CTA tracks. Route data sourced from CTA GTFS feeds
{% enddocs %}

## nearest_cta_route_name

{% docs column_nearest_cta_route_name %}
Nearest CTA route full name (`Red Line`, `Green Line`, etc.).

Routes include any active CTA tracks. Route data sourced from CTA GTFS feeds
{% enddocs %}

## nearest_cta_stop_dist_ft

{% docs column_nearest_cta_stop_dist_ft %}
Nearest CTA stop distance (feet).

Stops include any active CTA stops for trains only.
Stop data sourced from CTA GTFS feeds
{% enddocs %}

## nearest_cta_stop_id

{% docs column_nearest_cta_stop_id %}
Nearest CTA stop 5-digit internal ID.

Stops include any active CTA stops for trains only.
Stop data sourced from CTA GTFS feeds
{% enddocs %}

## nearest_cta_stop_name

{% docs column_nearest_cta_stop_name %}
Nearest CTA stop common name (`Harrison`, `Belmont`, etc.).

Stops include any active CTA stops for trains only.
Stop data sourced from CTA GTFS feeds
{% enddocs %}

## nearest_golf_course_dist_ft

{% docs column_nearest_golf_course_dist_ft %}
Nearest golf course distance (feet).

Golf course data sourced from Cook County GIS and OpenStreetMap
{% enddocs %}

## nearest_golf_course_id

{% docs column_nearest_golf_course_id %}
Nearest golf course ID, either Cook County ID or OSM ID.

Golf course data sourced from Cook County GIS and OpenStreetMap
{% enddocs %}

## nearest_grocery_osm_id

{% docs column_nearest_grocery_store_osm_id %}
Nearest grocery store ID number via OSM
{% enddocs %}

## nearest_grocery_store_dist_ft

{% docs column_nearest_grocery_store_dist_ft %}
Nearest grocery store distance (feet)
{% enddocs %}

## nearest_grocery_store_name

{% docs column_nearest_grocery_store_name %}
Nearest grocery store name via OSM
{% enddocs %}


## nearest_highway_road_name

{% docs column_nearest_highway_road_name %}
Nearest highway road name.

Road data sourced from Illinois Department of Transportation.
{% enddocs %}

## nearest_highway_road_dist_ft

{% docs column_nearest_highway_road_dist_ft %}
Distance to nearest highway road.

Road data sourced from Illinois Department of Transportation.
{% enddocs %}

## nearest_highway_road_daily_traffic

{% docs column_nearest_highway_road_daily_traffic %}
Daily traffic of nearest highway road.

Road data sourced from Illinois Department of Transportation.
{% enddocs %}

## nearest_highway_road_lanes

{% docs column_nearest_arterial_road_daily_traffic %}
Number of lanes for the nearest highway road.

Road data sourced from Illinois Department of Transportation.
{% enddocs %}

## nearest_highway_road_surface type

{% docs column_nearest_highway_road_daily_traffic %}
Surface type for the nearest highway road (for example brick, stone, etc.).

Road data sourced from Illinois Department of Transportation.
{% enddocs %}

## nearest_highway_road_speed_limit

{% docs column_nearest_highway_road_daily_traffic %}
Speed limit for the nearest highway road

Road data sourced from Illinois Department of Transportation
{% enddocs %}


## nearest_hospital_dist_ft

{% docs column_nearest_hospital_dist_ft %}
Nearest hospital distance (feet).

Hospital locations sourced from Cook County GIS
{% enddocs %}

## nearest_hospital_gnis_code

{% docs column_nearest_hospital_gnis_code %}
Nearest hospital GNIS code.

Hospital locations sourced from Cook County GIS
{% enddocs %}

## nearest_hospital_name

{% docs column_nearest_hospital_name %}
Nearest hospital full name.

Hospital locations sourced from Cook County GIS
{% enddocs %}

## nearest_major_road_dist_ft

{% docs column_nearest_major_road_dist_ft %}
Nearest major road distance (feet).

Major road locations sourced from OpenStreetMap (OSM).
Major roads include any OSM ways tagged with
`highway/motorway`, `highway/trunk`, or `highway/primary`
{% enddocs %}

## nearest_major_road_name

{% docs column_nearest_major_road_name %}
Nearest major road name, if available.

Major road locations sourced from OpenStreetMap (OSM).
Major roads include any OSM ways tagged with
`highway/motorway`, `highway/trunk`, or `highway/primary`
{% enddocs %}

## nearest_major_road_osm_id

{% docs column_nearest_major_road_osm_id %}
Nearest major road OpenStreetMap ID.

Major road locations sourced from OpenStreetMap (OSM).
Major roads include any OSM ways tagged with
`highway/motorway`, `highway/trunk`, or `highway/primary`
{% enddocs %}

## nearest_metra_route_dist_ft

{% docs column_nearest_metra_route_dist_ft %}
Nearest Metra route distance (feet).

Routes include any active Metra tracks. Route data sourced
from Metra GTFS feeds
{% enddocs %}

## nearest_metra_route_id

{% docs column_nearest_metra_route_id %}
Nearest Metra route short name (`RI`, `ME`, etc.).

Routes include any active Metra tracks. Route data sourced
from Metra GTFS feeds
{% enddocs %}

## nearest_metra_route_name

{% docs column_nearest_metra_route_name %}
Nearest Metra route full name (`Rock Island`, `Metra Electric`, etc.).

Routes include any active Metra tracks. Route data sourced
from Metra GTFS feeds
{% enddocs %}

## nearest_metra_stop_dist_ft

{% docs column_nearest_metra_stop_dist_ft %}
Nearest Metra stop distance (feet).

Stops include any active Metra stops. Stop data sourced from Metra GTFS feeds
{% enddocs %}

## nearest_metra_stop_id

{% docs column_nearest_metra_stop_id %}
Nearest Metra stop short name (`LSS`, `18TH-UP`, etc.).

Stops include any active Metra stops. Stop data sourced from Metra GTFS feeds
{% enddocs %}

## nearest_metra_stop_name

{% docs column_nearest_metra_stop_name %}
Nearest Metra stop full name (`LaSalle Street`, `18th Street`, etc.).

Stops include any active Metra stops. Stop data sourced from Metra GTFS feeds
{% enddocs %}

## nearest_neighbor_dist_ft

{% docs column_nearest_neighbor_dist_ft %}
Nearest neighboring parcel distance (feet)

These columns provide the three nearest neighbor PINs, starting
with `nearest_neighbor_1_dist_ft` (which is the nearest)
{% enddocs %}

## nearest_neighbor_pin10

{% docs column_nearest_neighbor_pin10 %}
Nearest neighboring parcel ID (PIN).

These columns provide the three nearest neighbor PINs and their
distance, starting with `nearest_neighbor_1_*` (which is the nearest)
{% enddocs %}

## nearest_new_construction_char_yrblt

{% docs column_nearest_new_construction_char_yrblt %}
Year built of the nearest new construction
{% enddocs %}

## nearest_new_construction_dist_ft

{% docs column_nearest_new_construction_dist_ft %}
Nearest new construction distance (feet)
{% enddocs %}

## nearest_new_construction_pin10

{% docs column_nearest_new_construction_pin10 %}
PIN10 of nearest new construction from CCAO data
{% enddocs %}

## nearest_park_dist_ft

{% docs column_nearest_park_dist_ft %}
Nearest park distance (feet).

Park locations sourced from OpenStreetMap using the tag `leisure/park`
{% enddocs %}

## nearest_park_name

{% docs column_nearest_park_name %}
Nearest park full name.

Park locations sourced from OpenStreetMap using the tag `leisure/park`
{% enddocs %}

## nearest_park_osm_id

{% docs column_nearest_park_osm_id %}
Nearest park OpenStreetMap ID.

Park locations sourced from OpenStreetMap using the tag `leisure/park`
{% enddocs %}

## nearest_railroad_dist_ft

{% docs column_nearest_railroad_dist_ft %}
Nearest railroad distance (feet).

Railroad locations sourced from Cook County GIS. Inclusive of any rail
(CTA, Metra, non-passenger freight, etc.)
{% enddocs %}

## nearest_railroad_id

{% docs column_nearest_railroad_id %}
Nearest railroad Cook County ID.

Railroad locations sourced from Cook County GIS. Inclusive of any rail
(CTA, Metra, non-passenger freight, etc.)
{% enddocs %}

## nearest_railroad_name

{% docs column_nearest_railroad_name %}
Nearest railroad line name, if available.

Railroad locations sourced from Cook County GIS. Inclusive of any rail
(CTA, Metra, non-passenger freight, etc.)
{% enddocs %}

## nearest_secondary_road_dist_ft

{% docs column_nearest_secondary_road_dist_ft %}
Nearest secondary road distance (feet).

Secondary road locations sourced from OpenStreetMap (OSM) and include
any OSM ways tagged with `highway/secondary`
{% enddocs %}

## nearest_secondary_road_name

{% docs column_nearest_secondary_road_name %}
Nearest secondary road name, if available.

Secondary road locations sourced from OpenStreetMap (OSM) and include
any OSM ways tagged with `highway/secondary`
{% enddocs %}

## nearest_secondary_road_osm_id

{% docs column_nearest_secondary_road_osm_id %}
Nearest secondary road OpenStreetMap ID.

Secondary road locations sourced from OpenStreetMap (OSM) and include
any OSM ways tagged with `highway/secondary`
{% enddocs %}

## nearest_stadium_dist_ft

{% docs column_nearest_stadium_dist_ft %}
Nearest stadium distance (feet).

Stadium locations sourced from Cook County GIS
{% enddocs %}

## nearest_stadium_name

{% docs column_nearest_stadium_name %}
Nearest stadium full name.

Stadium locations sourced from Cook County GIS
{% enddocs %}

## nearest_university_dist_ft

{% docs column_nearest_university_dist_ft %}
Nearest university distance (feet).

University locations sourced from Cook County GIS
{% enddocs %}

## nearest_university_gnis_code

{% docs column_nearest_university_gnis_code %}
Nearest university GNIS code.

University locations sourced from Cook County GIS
{% enddocs %}

## nearest_university_name

{% docs column_nearest_university_name %}
Nearest university full name.

University locations sourced from Cook County GIS
{% enddocs %}

## nearest_vacant_land_dist_ft

{% docs column_nearest_vacant_land_dist_ft %}
Nearest vacant land (class 100) parcel distance (feet).

Note that the parcel must be larger than 1,000 square feet.

Parcel locations sourced from Cook County parcel layer. Class sourced
from `iasworld.pardat`
{% enddocs %}

## nearest_vacant_land_pin10

{% docs column_nearest_vacant_land_pin10 %}
Nearest vacant land (class 100) 10-digit PIN.

Note that the parcel must be larger than 1,000 square feet.

Parcel locations sourced from Cook County parcel layer. Class sourced
from `iasworld.pardat`
{% enddocs %}

## nearest_water_dist_ft

{% docs column_nearest_water_dist_ft %}
Nearest water distance (feet).

Water locations are inclusive of _any_ body of water. Sourced from
Census hydrology files
{% enddocs %}

## nearest_water_id

{% docs column_nearest_water_id %}
Nearest water Census ID.

Water locations are inclusive of _any_ body of water. Sourced from
Census hydrology files
{% enddocs %}

## nearest_water_name

{% docs column_nearest_water_name %}
Nearest water name, if available.

Water locations are inclusive of _any_ body of water. Sourced from
Census hydrology files
{% enddocs %}

## num_bus_stop_in_half_mile

{% docs column_num_bus_stop_in_half_mile %}
Number of bus stops within half mile.

Includes CTA and PACE bus stops. Stop locations sourced from agency GTFS feeds
{% enddocs %}

## num_foreclosure_in_half_mile_past_5_years

{% docs column_num_foreclosure_in_half_mile_past_5_years %}
Number of foreclosures within half mile (past 5 years).

Sourced from Illinois Public Record (IPR). Note that this data is
reported on a long lag
{% enddocs %}

## num_foreclosure_per_1000_pin_past_5_years

{% docs column_num_foreclosure_per_1000_pin_past_5_years %}
Number of foreclosures per 1000 PINs, within half mile (past 5 years).

Normalized version of the half mile foreclosure count to account for PIN
density. Sourced from Illinois Public Record (IPR). Note that this data
is reported on a long lag
{% enddocs %}

## num_pin_in_half_mile

{% docs column_num_pin_in_half_mile %}
Number of PINs within half mile
{% enddocs %}

## num_school_in_half_mile

{% docs column_num_school_in_half_mile %}
Number of schools (any kind) within half mile.

School locations sourced from [GreatSchools](https://www.greatschools.org/)
{% enddocs %}

## num_school_with_rating_in_half_mile

{% docs column_num_school_with_rating_in_half_mile %}
Number of schools (any kind) within half mile.

Includes only schools that have a GreatSchools rating. School locations
and ratings sourced from [GreatSchools](https://www.greatschools.org/)
{% enddocs %}
