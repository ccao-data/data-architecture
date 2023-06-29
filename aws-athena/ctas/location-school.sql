CREATE TABLE IF NOT EXISTS location.school
WITH (
    FORMAT = 'Parquet',
    WRITE_COMPRESSION = 'SNAPPY',
    EXTERNAL_LOCATION = 's3://ccao-athena-ctas-us-east-1/location/school',
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

    distinct_years_rhs AS (
        SELECT DISTINCT year
        FROM spatial.school_district
        WHERE geoid IS NOT NULL
    ),

    distinct_joined AS (
        SELECT
            dp.x_3435,
            dp.y_3435,
            MAX(CASE
                WHEN school.district_type = 'elementary' THEN school.geoid
            END) AS school_elementary_district_geoid,
            MAX(CASE
                WHEN school.district_type = 'elementary' THEN school.name
            END) AS school_elementary_district_name,
            MAX(CASE
                WHEN school.district_type = 'secondary' THEN school.geoid
            END) AS school_secondary_district_geoid,
            MAX(CASE
                WHEN school.district_type = 'secondary' THEN school.name
            END) AS school_secondary_district_name,
            MAX(CASE
                WHEN school.district_type = 'unified' THEN school.geoid
            END) AS school_unified_district_geoid,
            MAX(CASE
                WHEN school.district_type = 'unified' THEN school.name
            END) AS school_unified_district_name,
            school.year
        FROM distinct_pins AS dp
        LEFT JOIN spatial.school_district AS school
            ON ST_WITHIN(
                ST_POINT(dp.x_3435, dp.y_3435),
                ST_GEOMFROMBINARY(school.geometry_3435)
            )
        GROUP BY dp.x_3435, dp.y_3435, school.year
    )

    SELECT
        pcl.pin10,
        dj.school_elementary_district_geoid,
        dj.school_elementary_district_name,
        dj.school_secondary_district_geoid,
        dj.school_secondary_district_name,
        dj.school_unified_district_geoid,
        dj.school_unified_district_name,
        CONCAT(CAST(CAST(dj.year AS INTEGER) - 1 AS VARCHAR), ' - ', dj.year)
            AS school_school_year,
        dj.year AS school_data_year,
        pcl.year
    FROM spatial.parcel AS pcl
    LEFT JOIN distinct_joined AS dj
        ON pcl.year = dj.year
        AND pcl.x_3435 = dj.x_3435
        AND pcl.y_3435 = dj.y_3435
    WHERE pcl.year >= (SELECT MIN(year) FROM distinct_years_rhs)
)
