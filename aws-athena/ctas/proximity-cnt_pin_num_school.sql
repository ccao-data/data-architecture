-- CTAS to create a table of schools counts and ratings. Public schools must be
-- within 1/2 mile AND in the same district as the target PIN. Private/charter
-- schools must be within 1/2 mile
CREATE TABLE IF NOT EXISTS proximity.cnt_pin_num_school
WITH (
    format='Parquet',
    write_compression = 'SNAPPY',
    external_location='s3://ccao-athena-ctas-us-east-1/proximity/cnt_pin_num_school',
    partitioned_by = ARRAY['year'],
    bucketed_by = ARRAY['pin10'],
    bucket_count = 1
) AS (
    WITH distinct_pins AS (
        SELECT DISTINCT x_3435, y_3435
        FROM spatial.parcel
    ),
    distinct_years AS (
        SELECT DISTINCT year
        FROM spatial.parcel
    ),
    distinct_years_rhs AS (
        SELECT DISTINCT year
        FROM other.gs_school_rating
    ),
    school_locations_public AS (
        SELECT fill_years.pin_year, fill_data.*
        FROM (
            SELECT dy.year AS pin_year, MAX(df.year) AS fill_year
            FROM other.gs_school_rating df
            CROSS JOIN distinct_years dy
            WHERE dy.year >= df.year
            GROUP BY dy.year
        ) fill_years
        LEFT JOIN other.gs_school_rating fill_data
            ON fill_years.fill_year = fill_data.year
        WHERE type = 'public'
    ),
    school_locations_other AS (
        SELECT fill_years.pin_year, fill_data.*
        FROM (
            SELECT dy.year AS pin_year, MAX(df.year) AS fill_year
            FROM other.gs_school_rating df
            CROSS JOIN distinct_years dy
            WHERE dy.year >= df.year
            GROUP BY dy.year
        ) fill_years
        LEFT JOIN other.gs_school_rating fill_data
            ON fill_years.fill_year = fill_data.year
        WHERE type != 'public'
    ),
    school_ratings AS (
        SELECT DISTINCT
            p.x_3435,
            p.y_3435,
            pub.rating,
            pub.pin_year,
            pub.year
        FROM distinct_pins p
        INNER JOIN spatial.school_district dis
            ON ST_Contains(
                ST_GeomFromBinary(dis.geometry_3435),
                ST_Point(p.x_3435, p.y_3435)
            )
        INNER JOIN school_locations_public pub
            ON ST_Contains(
                ST_Buffer(ST_GeomFromBinary(pub.geometry_3435), 2640),
                ST_Point(p.x_3435, p.y_3435)
            )
            AND ST_Contains(
                ST_GeomFromBinary(dis.geometry_3435),
                ST_GeomFromBinary(pub.geometry_3435)
            )
            AND dis.geoid = pub.geoid
        UNION ALL
        SELECT
            p.x_3435,
            p.y_3435,
            oth.rating,
            oth.pin_year,
            oth.year
        FROM distinct_pins p
        INNER JOIN school_locations_other oth
            ON ST_Contains(
                ST_Buffer(ST_GeomFromBinary(oth.geometry_3435), 2640),
                ST_Point(p.x_3435, p.y_3435)
            )
    ),
    school_ratings_agg AS (
        SELECT
            pin_year,
            x_3435, y_3435,
            COUNT(*) AS num_school_in_half_mile,
            AVG(rating) AS avg_school_rating_in_half_mile,
            MAX(year) AS num_school_data_year
        FROM school_ratings
        GROUP BY x_3435, y_3435, pin_year
    )
    SELECT
        p.pin10,
        CASE
            WHEN sr.num_school_in_half_mile IS NULL THEN 0
            ELSE sr.num_school_in_half_mile
        END AS num_school_in_half_mile,
        sr.avg_school_rating_in_half_mile,
        sr.num_school_data_year,
        p.year
    FROM spatial.parcel p
    LEFT JOIN school_ratings_agg sr
        ON p.x_3435 = sr.x_3435
        AND p.y_3435 = sr.y_3435
        AND p.year = sr.pin_year
    WHERE p.year >= (SELECT MIN(year) FROM distinct_years_rhs)
)