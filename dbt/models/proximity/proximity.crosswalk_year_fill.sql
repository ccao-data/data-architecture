/*
Table detailing which years of proximity data are available and
should be joined to each year of assessment data. Assessment years
missing equivalent proximity data are filled thus:

1. All historical data is filled FORWARD in time, i.e. data from 2020 fills
   2021.
2. Current data is filled BACKWARD to account for missing historical data.

The output of this query contains three different variations on "year" columns:

1. `year` is the PIN year; there should be one `year` for every year that
   exists in `spatial.parcel`
2. `{feature_name}_fill_year` is the year of data for a PIN in the input data
   that should be used to fill records with a given `year`
3. `{feature_name}_data_year` is the original year in the raw data that
   produced the value that exists in the fill year

See the `crosswalk_year_fill` macro for more details on what these columns mean
and how we construct them.
*/
{{ config(materialized='table') }}

{#-
    Mapping containing the fields we want to fill, where each key is the
    name of a table in the `proximity` schema and each value is a list of names
    of `*_data_year` fields in that table that we want to use for filling
-#}
{%- set fields = {
    "cnt_pin_num_bus_stop": ["num_bus_stop"],
    "cnt_pin_num_foreclosure": ["num_foreclosure"],
    "cnt_pin_num_school": ["num_school", "num_school_rating"],
    "dist_pin_to_airport": ["airport"],
    "dist_pin_to_bike_trail": ["nearest_bike_trail"],
    "dist_pin_to_cemetery": ["nearest_cemetery"],
    "dist_pin_to_cta_route": ["nearest_cta_route"],
    "dist_pin_to_cta_stop": ["nearest_cta_stop"],
    "dist_pin_to_golf_course": ["nearest_golf_course"],
    "dist_pin_to_grocery_store": ["nearest_grocery_store"],
    "dist_pin_to_hospital": ["nearest_hospital"],
    "dist_pin_to_lake_michigan": ["lake_michigan"],
    "dist_pin_to_major_road": ["nearest_major_road"],
    "dist_pin_to_metra_route": ["nearest_metra_route"],
    "dist_pin_to_metra_stop": ["nearest_metra_stop"],
    "dist_pin_to_new_construction": ["nearest_new_construction"],
    "dist_pin_to_park": ["nearest_park"],
    "dist_pin_to_railroad": ["nearest_railroad"],
    "dist_pin_to_road_arterial": ["nearest_road_arterial"],
    "dist_pin_to_road_collector": ["nearest_road_collector"],
    "dist_pin_to_road_highway": ["nearest_road_highway"],
    "dist_pin_to_secondary_road": ["nearest_secondary_road"],
    "dist_pin_to_stadium": ["nearest_stadium"],
    "dist_pin_to_university": ["nearest_university"],
    "dist_pin_to_vacant_land": ["nearest_vacant_land"],
    "dist_pin_to_water": ["nearest_water"]
} -%}

{{ crosswalk_year_fill("proximity", fields) }}
