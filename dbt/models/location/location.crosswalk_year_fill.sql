/*
Table detailing which years of location data are available and
should be joined to each year of assessment data. Assessment years missing
equivalent location data are filled thus:

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
    name of a table in the `location` schema and each value is a list of names
    of `*_data_year` fields in that table that we want to use for filling
-#}
{%- set fields = {
    "census": ["census"],
    "census_acs5": ["census_acs5"],
    "political": [
        "cook_board_of_review_district",
        "cook_commissioner_district",
        "cook_judicial_district",
        "cook_municipality",
        "ward_chicago",
        "ward_evanston"
    ],
    "chicago": [
        "chicago_community_area",
        "chicago_industrial_corridor",
        "chicago_police_district"
    ],
    "economy": [
        "econ_coordinated_care_area",
        "econ_enterprise_zone",
        "econ_industrial_growth_zone",
        "econ_qualified_opportunity_zone",
        "econ_central_business_district"
    ],
    "environment": [
        "env_flood_fema",
        "env_flood_fs",
        "env_ohare_noise_contour",
        "env_airport_noise"
    ],
    "school": ["school"],
    "tax": ["tax"],
    "access": ["access_cmap_walk"],
    "other": ["misc_subdivision"]
} -%}

{{ crosswalk_year_fill("location", fields) }}
