-- CTAS to create a table of schools counts and ratings. Public schools must be
-- within 1/2 mile AND in the same district as the target PIN. Private/charter
-- schools must be within 1/2 mile
{{
    config(
        materialized='table',
        partitioned_by=['year'],
        bucketed_by=['pin10'],
        bucket_count=1
    )
}}

WITH cnt_pin_num_school AS (
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
        FROM {{ source('other', 'great_schools_rating') }}
    ),

    school_locations AS (
        SELECT
            fill_years.pin_year,
            fill_data.*
        FROM (
            SELECT
                dy.year AS pin_year,
                MAX(df.year) AS fill_year
            FROM {{ source('other', 'great_schools_rating') }} AS df
            CROSS JOIN distinct_years AS dy
            WHERE dy.year >= df.year
            GROUP BY dy.year
        ) AS fill_years
        LEFT JOIN {{ source('other', 'great_schools_rating') }} AS fill_data
            ON fill_years.fill_year = fill_data.year
    ),

    school_locations_public AS (
        SELECT *
        FROM school_locations
        WHERE type = 'public'
    ),

    school_locations_other AS (
        SELECT *
        FROM school_locations
        WHERE type != 'public'
    ),

    school_ratings AS (
        SELECT DISTINCT
            dp.x_3435,
            dp.y_3435,
            pub.rating,
            pub.pin_year,
            pub.year
        FROM distinct_pins AS dp
        -- Keep only public schools with 1/2 mile WITHIN each PIN's district
        INNER JOIN {{ source('spatial', 'school_district') }} AS dis
            ON ST_CONTAINS(
                ST_GEOMFROMBINARY(dis.geometry_3435),
                ST_POINT(dp.x_3435, dp.y_3435)
            )
        INNER JOIN school_locations_public AS pub
            ON ST_CONTAINS(
                ST_BUFFER(ST_GEOMFROMBINARY(pub.geometry_3435), 2640),
                ST_POINT(dp.x_3435, dp.y_3435)
            )
        WHERE dis.geoid = pub.district_geoid
        UNION ALL
        -- Any and all private schools within 1/2 mile
        SELECT
            dp.x_3435,
            dp.y_3435,
            oth.rating,
            oth.pin_year,
            oth.year
        FROM distinct_pins AS dp
        INNER JOIN school_locations_other AS oth
            ON ST_CONTAINS(
                ST_BUFFER(ST_GEOMFROMBINARY(oth.geometry_3435), 2640),
                ST_POINT(dp.x_3435, dp.y_3435)
            )
    ),

    school_ratings_agg AS (
        SELECT
            pin_year,
            x_3435,
            y_3435,
            COUNT(*) AS num_school_in_half_mile,
            SUM(
                CASE
                    WHEN rating IS NOT NULL THEN 1
                    ELSE 0
                END
            ) AS num_school_with_rating_in_half_mile,
            AVG(rating) AS avg_school_rating_in_half_mile,
            MAX(year) AS num_school_data_year,
            MAX(year) AS num_school_rating_data_year
        FROM school_ratings
        GROUP BY x_3435, y_3435, pin_year
    )

    SELECT
        pcl.pin10,
        COALESCE(sr.num_school_in_half_mile, 0) AS num_school_in_half_mile,
        COALESCE(
            sr.num_school_with_rating_in_half_mile,
            0
        ) AS num_school_with_rating_in_half_mile,
        sr.avg_school_rating_in_half_mile,
        sr.num_school_data_year,
        sr.num_school_rating_data_year,
        pcl.year
    FROM {{ source('spatial', 'parcel') }} AS pcl
    LEFT JOIN school_ratings_agg AS sr
        ON pcl.x_3435 = sr.x_3435
        AND pcl.y_3435 = sr.y_3435
        AND pcl.year = sr.pin_year
    WHERE pcl.year >= (SELECT MIN(year) FROM distinct_years_rhs)
)

SELECT * FROM cnt_pin_num_school
