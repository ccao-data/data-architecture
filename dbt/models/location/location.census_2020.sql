-- This view only exists since the IHS House Price Index uses 2020 geoids for
-- for all years.

{{
    config(
        materialized='table',
        partitioned_by=['year'],
        bucketed_by=['pin10'],
        bucket_count=1
    )
}}

WITH distinct_pins AS (
    SELECT DISTINCT
        x_3435,
        y_3435
    FROM {{ source('spatial', 'parcel') }}
),

distinct_joined AS (
    SELECT
        dp.x_3435,
        dp.y_3435,
        MAX(CASE
            WHEN cen.geography = 'block_group' THEN cen.geoid
        END) AS census_block_group_geoid,
        MAX(CASE
            WHEN cen.geography = 'block' THEN cen.geoid
        END) AS census_block_geoid,
        MAX(CASE
            WHEN cen.geography = 'congressional_district' THEN cen.geoid
        END) AS census_congressional_district_geoid,
        MAX(CASE
            WHEN cen.geography = 'county_subdivision' THEN cen.geoid
        END) AS census_county_subdivision_geoid,
        MAX(CASE
            WHEN cen.geography = 'place' THEN cen.geoid
        END) AS census_place_geoid,
        MAX(CASE
            WHEN cen.geography = 'puma' THEN cen.geoid
        END) AS census_puma_geoid,
        MAX(CASE
            WHEN cen.geography = 'school_district_elementary' THEN cen.geoid
        END) AS census_school_district_elementary_geoid,
        MAX(CASE
            WHEN cen.geography = 'school_district_secondary' THEN cen.geoid
        END) AS census_school_district_secondary_geoid,
        MAX(CASE
            WHEN cen.geography = 'school_district_unified' THEN cen.geoid
        END) AS census_school_district_unified_geoid,
        MAX(CASE
            WHEN cen.geography = 'state_representative' THEN cen.geoid
        END) AS census_state_representative_geoid,
        MAX(CASE
            WHEN cen.geography = 'state_senate' THEN cen.geoid
        END) AS census_state_senate_geoid,
        MAX(CASE
            WHEN cen.geography = 'tract' THEN cen.geoid
        END) AS census_tract_geoid,
        MAX(CASE
            WHEN cen.geography = 'zcta' THEN cen.geoid
        END) AS census_zcta_geoid,
        cen.year
    FROM distinct_pins AS dp
    LEFT JOIN {{ source('spatial', 'census') }} AS cen
        ON ST_WITHIN(
            ST_POINT(dp.x_3435, dp.y_3435),
            ST_GEOMFROMBINARY(cen.geometry_3435)
        )
    WHERE cen.year = '2022' -- PUMA spatial data isn't available before 2012
    GROUP BY dp.x_3435, dp.y_3435, cen.year
)

SELECT
    pcl.pin10,
    dj.census_block_group_geoid,
    dj.census_block_geoid,
    dj.census_congressional_district_geoid,
    dj.census_county_subdivision_geoid,
    dj.census_place_geoid,
    dj.census_puma_geoid,
    dj.census_school_district_elementary_geoid,
    dj.census_school_district_secondary_geoid,
    dj.census_school_district_unified_geoid,
    dj.census_state_representative_geoid,
    dj.census_state_senate_geoid,
    dj.census_tract_geoid,
    dj.census_zcta_geoid,
    dj.year AS census_data_year,
    pcl.year
FROM {{ source('spatial', 'parcel') }} AS pcl
LEFT JOIN distinct_joined AS dj
    ON pcl.x_3435 = dj.x_3435
    AND pcl.y_3435 = dj.y_3435
