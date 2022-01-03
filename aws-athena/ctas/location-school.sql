CREATE TABLE IF NOT EXISTS location.school
WITH (
    format='Parquet',
    write_compression = 'SNAPPY',
    external_location='s3://ccao-athena-ctas-us-east-1/location/school',
    partitioned_by = ARRAY['year'],
    bucketed_by = ARRAY['pin10'],
    bucket_count = 1
) AS (
    WITH distinct_pins AS (
        SELECT DISTINCT x_3435, y_3435
        FROM spatial.parcel
        WHERE year >= '2012'
    ),
    distinct_joined AS (
        SELECT
            p.x_3435, p.y_3435,
        MAX(CASE
            WHEN school.district_type = 'elementary' THEN school.geoid
            ELSE NULL END) AS school_elementary_district_geoid,
        MAX(CASE
            WHEN school.district_type = 'elementary' THEN school.name
            ELSE NULL END) AS school_elementary_district_name,
        MAX(CASE
            WHEN school.district_type = 'secondary' THEN school.geoid
            ELSE NULL END) AS school_secondary_district_geoid,
        MAX(CASE
            WHEN school.district_type = 'secondary' THEN school.name
            ELSE NULL END) AS school_secondary_district_name,
        MAX(CASE
            WHEN school.district_type = 'unified' THEN school.geoid
            ELSE NULL END) AS school_unified_district_geoid,
        MAX(CASE
            WHEN school.district_type = 'unified' THEN school.name
            ELSE NULL END) AS school_unified_district_name,
        school.year
        FROM distinct_pins p
        LEFT JOIN spatial.school_district school
            ON ST_Within(ST_Point(p.x_3435, p.y_3435), ST_GeomFromBinary(school.geometry_3435))
        GROUP BY p.x_3435, p.y_3435, school.year
    )
    SELECT
        p.pin10,
        school_elementary_district_geoid, school_elementary_district_name,
        school_secondary_district_geoid, school_secondary_district_name,
        school_unified_district_geoid, school_unified_district_name,
        CONCAT(CAST(CAST(dj.year AS integer) - 1 AS varchar), ' - ', dj.year) AS school_school_year,
        dj.year AS school_data_year,
        p.year
    FROM spatial.parcel p
    LEFT JOIN distinct_joined dj
        ON p.year = dj.year
        AND p.x_3435 = dj.x_3435
        AND p.y_3435 = dj.y_3435
    WHERE p.year >= '2012'
)