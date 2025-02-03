/*
Table detailing which years of location data are available and
should be joined to each year of assessment data. Assessment years missing
equivalent location data are filled thus:

1. All historical data is filled FORWARD in time, i.e. data from 2020 fills
   2021.
2. Current data is filled BACKWARD to account for missing historical data.

Note that we assume that source data are already forward-filled, i.e. that
we have already completed step 1 for all data sources before they reach this
query. As such, this query only completes step 2 of the above list, but its
output data will be filled in both directions.

The various `year` columns have different meanings:

* `year` is the PIN year
* `*_fill_year` is the PIN year of a data source
* `*_data_year` is the year of data that we filled using the data source

Take this hypothetical row as an example:

| year | census_fill_year | census_data_year |
| ---- | ---------------- | ---------------- |
| 2019 | 2015             | 2017             |

This row intends to communicate that for a PIN in year 2010, a data consumer
can get a filled value for a `location.census` column by joining to the
representation of that PIN in the `location.census` table with year 2015, and
that this particular row of data in `location.census` uses a value that was
recorded in the raw data in 2017.

So if we want to use the crosswalk to fill data in this case, we can write a
query like this:

    SELECT *
    FROM spatial.parcel AS pin
    INNER JOIN location.crosswalk_year_fill AS cyf
        ON pin.year = cyf.year
    LEFT JOIN location.census AS census
        ON pin.pin10 = census.pin10
        AND cyf.census_fill_year = census.year
*/
{{ config(materialized='table') }}

{#-
    These are all of the fields we want to fill, where each key is the name
    of a table in the `location` schema and each value is a list of names of
    `*_data_year` fields in that table that we want to use for filling
-#}
{%- set fields = {
    "census": ["census"],
    "census_acs5": ["census_acs5"],
    "political": [
        "cook_board_of_review_district",
        "cook_commissioner_district",
        "cook_judicial_district",
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

/*
For each year of data we have in `spatial.parcel`, get the corresponding
PIN year and data year from our source tables. We expect that source tables
will already be filled up to their earliest date of available data.

As an example, assume we have Census data for the year 2020, but no other years
aside from that year; also, assume that we have parcel data for years
2018-2022. In this hypothetical case, the output of this subquery will look
like this if we filter for only Census-related fields:

  | year | census_fill_year | census_data_year |
  | ---- | ---------------- | ---------------- |
  | 2018 |                  |                  |
  | 2019 |                  |                  |
  | 2020 | 2020             | 2020             |
  | 2021 | 2021             | 2020             |
  | 2022 | 2022             | 2020             |
*/
WITH unfilled AS (
    SELECT
        pin.year,
        {% for tablename, fieldnames in fields.items() %}
            {%- set outer_loop_last = loop.last %}
            {% for fieldname in fieldnames %}
                {%- set inner_loop_last = loop.last %}
                    {#-
                        Use MAX to remove nulls in cases of years where some PINs
                        have no match in the external data, since the DISTINCT
                        subqueries in the list of joins below will still return
                        those nulls
                    -#}
                MAX({{ tablename }}.year) AS {{ fieldname }}_fill_year,
                MAX({{ tablename }}.{{ fieldname }}_data_year)
                    AS {{ fieldname }}_data_year
                {%- if not outer_loop_last or not inner_loop_last -%}
                    ,
                {%- endif -%}
            {% endfor %}
        {% endfor %}
    FROM (
        SELECT DISTINCT year
        FROM {{ source('spatial', 'parcel') }}
    ) AS pin
    {% for tablename, fieldnames in fields.items() %}
        LEFT JOIN (
            SELECT DISTINCT
                year,
                {% for fieldname in fieldnames %}
                    {{ fieldname }}_data_year
                    {%- if not loop.last -%}
                        ,
                    {%- endif -%}
                {% endfor %}
            FROM {{ ref('location.' + tablename) }}
        ) AS {{ tablename }}
            ON pin.year = {{ tablename }}.year
    {% endfor %}
    GROUP BY pin.year
)

/*
Now that we have all parcel years and the earliest year of data in our
location data sources, we can fill in all parcel years before any data were
recorded by using the earliest year of available data.

Taking our earlier example again, here's the output we expect for only
Census-related fields (note that the missing years 2018-2019 are now filled):

  | year | census_fill_year | census_data_year |
  | ---- | ---------------- | ---------------- |
  | 2018 | 2020             | 2020             |
  | 2019 | 2020             | 2020             |
  | 2020 | 2020             | 2020             |
  | 2021 | 2021             | 2020             |
  | 2022 | 2022             | 2020             |
*/
SELECT
    unfilled.year,
    {% for tablename, fieldnames in fields.items() %}
        {%- set outer_loop_last = loop.last %}
        {% for fieldname in fieldnames %}
            {%- set inner_loop_last = loop.last %}
            COALESCE(
                {{ fieldname }}_fill_year,
                {#-
                    Use a window function to fill nulls with the first year of
                    available data for this data source
                -#}
                LAST_VALUE({{ fieldname }}_fill_year)
                    IGNORE NULLS
                    OVER (ORDER BY unfilled.year DESC)
            ) AS {{ fieldname }}_fill_year,
            COALESCE(
                {{ fieldname }}_data_year,
                LAST_VALUE({{ fieldname }}_data_year)
                    IGNORE NULLS
                    OVER (ORDER BY unfilled.year DESC)
            ) AS {{ fieldname }}_data_year
            {%- if not outer_loop_last or not inner_loop_last -%}
                ,
            {%- endif -%}
        {% endfor %}
    {% endfor %}
FROM unfilled
ORDER BY unfilled.year
