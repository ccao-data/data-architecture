-- CTAS to create a table of schools counts and ratings. Public schools must be
-- within 1/2 mile AND in the same district as the target PIN. Private/charter
-- schools must be within 1/2 mile
CREATE TABLE IF NOT EXISTS proximity.cnt_pin_num_school
WITH (
    FORMAT = 'Parquet',
    WRITE_COMPRESSION = 'SNAPPY',
    EXTERNAL_LOCATION
    = 's3://ccao-athena-ctas-us-east-1/proximity/cnt_pin_num_school',
    PARTITIONED_BY = ARRAY['year'],
    BUCKETED_BY = ARRAY['pin10'],
    BUCKET_COUNT = 1
) AS (
    WITH distinct_pins AS (
        SELECT DISTINCT
            x_3435,
            y_3435
        FROM spatial.parcel
    ),

    distinct_years AS (
        SELECT DISTINCT year
        FROM spatial.parcel
    ),

    distinct_years_rhs AS (
        SELECT DISTINCT year
        FROM other.great_schools_rating
    ),

    school_locations AS (
        SELECT
            fill_years.pin_year,
            fill_data.*
        FROM (
            SELECT
                dy.year AS pin_year,
                MAX(df.year) AS fill_year
            FROM other.great_schools_rating AS df
            CROSS JOIN distinct_years AS dy
            WHERE dy.year >= df.year
            GROUP BY dy.year
        ) AS fill_years
        LEFT JOIN other.great_schools_rating AS fill_data
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
            p.x_3435,
            p.y_3435,
            pub.rating,
            pub.pin_year,
            pub.year
        FROM distinct_pins AS p
        -- Keep only public schools with 1/2 mile WITHIN each PIN's district
        INNER JOIN spatial.school_district AS dis
            ON ST_CONTAINS(
                ST_GEOMFROMBINARY(dis.geometry_3435),
                ST_POINT(p.x_3435, p.y_3435)
            )
        INNER JOIN school_locations_public AS pub
            ON ST_CONTAINS(
                ST_BUFFER(ST_GEOMFROMBINARY(pub.geometry_3435), 2640),
                ST_POINT(p.x_3435, p.y_3435)
            )
        WHERE dis.geoid = pub.district_geoid
        UNION ALL
        -- Any and all private schools within 1/2 mile
        SELECT
            p.x_3435,
            p.y_3435,
            oth.rating,
            oth.pin_year,
            oth.year
        FROM distinct_pins AS p
        INNER JOIN school_locations_other AS oth
            ON ST_CONTAINS(
                ST_BUFFER(ST_GEOMFROMBINARY(oth.geometry_3435), 2640),
                ST_POINT(p.x_3435, p.y_3435)
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
        p.pin10,
        COALESCE(sr.num_school_in_half_mile, 0) AS num_school_in_half_mile,
        COALESCE(
            sr.num_school_with_rating_in_half_mile,
            0
        ) AS num_school_with_rating_in_half_mile,
        sr.avg_school_rating_in_half_mile,
        sr.num_school_data_year,
        sr.num_school_rating_data_year,
        p.year
    FROM spatial.parcel AS p
    LEFT JOIN school_ratings_agg AS sr
        ON p.x_3435 = sr.x_3435
        AND p.y_3435 = sr.y_3435
        AND p.year = sr.pin_year
    WHERE p.year >= (SELECT MIN(year) FROM distinct_years_rhs)
)