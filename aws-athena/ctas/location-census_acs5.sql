-- CTAS specifically to facilitate attribute joins of PRIOR YEAR ACS data to
-- current PINs. For example, 2020 ACS data is not yet release, but we do have
-- current (2021) geographies and PINs. We need to join 2019 geographies to
-- 2021 PINs to facilitate joining 2019 ACS5 data
{{
    config(
        materialized='table',
        partitioned_by=['year'],
        bucketed_by=['pin10'],
        bucket_count=1
    )
}}

WITH census_acs5 AS (
    WITH distinct_pins AS (
        SELECT DISTINCT
            x_3435,
            y_3435
        FROM {{ source('spatial', 'parcel') }}
    ),

    distinct_years AS (
        SELECT DISTINCT year
        FROM {{ source('spatial', 'parcel') }}
    ),

    distinct_years_rhs AS (
        SELECT DISTINCT year
        FROM {{ source('spatial', 'census') }}
    ),

    acs5_max_year AS (
        SELECT MAX(year) AS max_year
        FROM {{ source('census', 'acs5') }}
    ),

    acs5_year_fill AS (
        SELECT
            dy.year AS pin_year,
            MAX(df.year) AS fill_year
        FROM {{ source('census', 'acs5') }} AS df
        CROSS JOIN distinct_years AS dy
        WHERE dy.year >= df.year
        GROUP BY dy.year
    ),

    distinct_joined AS (
        SELECT
            dp.x_3435,
            dp.y_3435,
            MAX(CASE
                WHEN cen.geography = 'congressional_district' THEN cen.geoid
            END) AS census_acs5_congressional_district_geoid,
            MAX(CASE
                WHEN cen.geography = 'county_subdivision' THEN cen.geoid
            END) AS census_acs5_county_subdivision_geoid,
            MAX(CASE
                WHEN cen.geography = 'place' THEN cen.geoid
            END) AS census_acs5_place_geoid,
            MAX(CASE
                WHEN cen.geography = 'puma' THEN cen.geoid
            END) AS census_acs5_puma_geoid,
            MAX(CASE
                WHEN cen.geography = 'school_district_elementary' THEN cen.geoid
            END) AS census_acs5_school_district_elementary_geoid,
            MAX(CASE
                WHEN cen.geography = 'school_district_secondary' THEN cen.geoid
            END) AS census_acs5_school_district_secondary_geoid,
            MAX(CASE
                WHEN cen.geography = 'school_district_unified' THEN cen.geoid
            END) AS census_acs5_school_district_unified_geoid,
            MAX(CASE
                WHEN cen.geography = 'state_representative' THEN cen.geoid
            END) AS census_acs5_state_representative_geoid,
            MAX(CASE
                WHEN cen.geography = 'state_senate' THEN cen.geoid
            END) AS census_acs5_state_senate_geoid,
            MAX(CASE
                WHEN cen.geography = 'tract' THEN cen.geoid
            END) AS census_acs5_tract_geoid,
            cen.year
        FROM distinct_pins AS dp
        LEFT JOIN (
            SELECT *
            FROM {{ source('spatial', 'census') }}
            WHERE year <= (SELECT max_year FROM acs5_max_year)
        ) AS cen
            ON ST_WITHIN(
                ST_POINT(dp.x_3435, dp.y_3435),
                ST_GEOMFROMBINARY(cen.geometry_3435)
            )
        GROUP BY dp.x_3435, dp.y_3435, cen.year
    )

    SELECT
        pcl.pin10,
        dj.census_acs5_congressional_district_geoid,
        dj.census_acs5_county_subdivision_geoid,
        dj.census_acs5_place_geoid,
        dj.census_acs5_puma_geoid,
        dj.census_acs5_school_district_elementary_geoid,
        dj.census_acs5_school_district_secondary_geoid,
        dj.census_acs5_school_district_unified_geoid,
        dj.census_acs5_state_representative_geoid,
        dj.census_acs5_state_senate_geoid,
        dj.census_acs5_tract_geoid,
        dj.year AS census_acs5_data_year,
        pcl.year
    FROM {{ source('spatial', 'parcel') }} AS pcl
    LEFT JOIN acs5_year_fill AS ayf
        ON pcl.year = ayf.pin_year
    LEFT JOIN distinct_joined AS dj
        ON ayf.fill_year = dj.year
        AND pcl.x_3435 = dj.x_3435
        AND pcl.y_3435 = dj.y_3435
    WHERE pcl.year >= (SELECT MIN(year) FROM distinct_years_rhs)
)

SELECT * FROM census_acs5
