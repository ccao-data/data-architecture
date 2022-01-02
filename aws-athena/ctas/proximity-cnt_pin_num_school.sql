-- CTAS to create a table of schools counts and ratings. Public schools must be
-- within 1/2 mile AND in the same district as the target PIN. Private/charter
-- schools must be within 1/2 mile
CREATE TABLE IF NOT EXISTS proximity.cnt_pin_num_school
WITH (
    format='Parquet',
    write_compression = 'SNAPPY',
    external_location='s3://ccao-athena-ctas-us-east-1/proximity/cnt_pin_num_school',
    partitioned_by = ARRAY['year']
) AS (
    WITH pin_locations AS (
        SELECT p.year, p.pin10, s.geoid, s.district_type, ST_Point(p.x_3435, p.y_3435) AS centroid
        FROM spatial.parcel p
        LEFT JOIN spatial.school_district s
            ON p.year = s.year
            AND ST_Contains(ST_GeomFromBinary(s.geometry_3435), ST_Point(p.x_3435, p.y_3435))
        WHERE p.year >= CAST((SELECT MAX(year) + 1 FROM other.gs_school_rating) AS varchar)
    ),
    school_locations_public AS (
        SELECT *
        FROM other.gs_school_rating
        WHERE type = 'public'
    ),
    school_locations_other AS (
        SELECT *
        FROM other.gs_school_rating
        WHERE type != 'public'
    ),
    school_ratings AS (
        SELECT p.year, p.pin10, pub.school_name, pub.rating
        FROM pin_locations p
        INNER JOIN school_locations_public pub
            ON ST_Contains(
                ST_Buffer(ST_GeomFromBinary(pub.geometry_3435), 2640), centroid
            )
            AND p.year = CAST((pub.year + 1) AS varchar)
            AND p.geoid = pub.geoid
        UNION ALL
        SELECT p.year, p.pin10,  oth.school_name, oth.rating
        FROM pin_locations p
        INNER JOIN school_locations_other oth
            ON ST_Contains(
                ST_Buffer(ST_GeomFromBinary(oth.geometry_3435), 2640), centroid
            )
            AND p.year = CAST((oth.year + 1) AS varchar)
    ),
    school_ratings_agg AS (
        SELECT
            year,
            pin10,
            COUNT(*) AS num_schools_in_half_mile,
            AVG(rating) AS avg_rating_in_half_mile
        FROM school_ratings
        GROUP BY pin10, year
    )
    SELECT
        p.pin10,
        CASE
            WHEN sr.num_schools_in_half_mile IS NULL THEN 0
            ELSE sr.num_schools_in_half_mile END AS num_schools_in_half_mile,
        sr.avg_rating_in_half_mile,
        p.year
    FROM spatial.parcel p
    LEFT JOIN school_ratings_agg sr
        ON p.pin10 = sr.pin10
        AND p.year = sr.year
    WHERE p.year >= CAST((SELECT MAX(year) + 1 FROM other.gs_school_rating) AS varchar)
)