CREATE TABLE IF NOT EXISTS location.census
WITH (
    format='Parquet',
    write_compression = 'SNAPPY',
    external_location='s3://ccao-athena-ctas-us-east-1/location/census',
    partitioned_by = ARRAY['year']
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
                WHEN cen.geography = 'block_group' THEN cen.geoid
                ELSE NULL END) AS census_geoid_block_group,
            MAX(CASE
                WHEN cen.geography = 'block' THEN cen.geoid
                ELSE NULL END) AS census_geoid_block,
            MAX(CASE
                WHEN cen.geography = 'congressional_district' THEN cen.geoid
                ELSE NULL END) AS census_geoid_congressional_district,
            MAX(CASE
                WHEN cen.geography = 'county_subdivision' THEN cen.geoid
                ELSE NULL END) AS census_geoid_county_subdivision,
            MAX(CASE
                WHEN cen.geography = 'place' THEN cen.geoid
                ELSE NULL END) AS census_geoid_place,
            MAX(CASE
                WHEN cen.geography = 'puma' THEN cen.geoid
                ELSE NULL END) AS census_geoid_puma,
            MAX(CASE
                WHEN cen.geography = 'school_district_elementary' THEN cen.geoid
                ELSE NULL END) AS census_geoid_school_district_elementary,
            MAX(CASE
                WHEN cen.geography = 'school_district_secondary' THEN cen.geoid
                ELSE NULL END) AS census_geoid_school_district_secondary,
            MAX(CASE
                WHEN cen.geography = 'school_district_unified' THEN cen.geoid
                ELSE NULL END) AS census_geoid_school_district_unified,
            MAX(CASE
                WHEN cen.geography = 'state_representative' THEN cen.geoid
                ELSE NULL END) AS census_geoid_state_representative,
            MAX(CASE
                WHEN cen.geography = 'state_senate' THEN cen.geoid
                ELSE NULL END) AS census_geoid_state_senate,
            MAX(CASE
                WHEN cen.geography = 'tract' THEN cen.geoid
                ELSE NULL END) AS census_geoid_tract,
            MAX(CASE
                WHEN cen.geography = 'zcta' THEN cen.geoid
                ELSE NULL END) AS census_geoid_zcta,
            cen.year
        FROM distinct_pins p
        LEFT JOIN spatial.census cen
            ON ST_Within(ST_Point(p.x_3435, p.y_3435), ST_GeomFromBinary(cen.geometry_3435))
        GROUP BY p.x_3435, p.y_3435, cen.year
    )
    SELECT
        p.pin10,
        census_geoid_block_group,
        census_geoid_block,
        census_geoid_congressional_district,
        census_geoid_county_subdivision,
        census_geoid_place,
        census_geoid_puma,
        census_geoid_school_district_elementary,
        census_geoid_school_district_secondary,
        census_geoid_school_district_unified,
        census_geoid_state_representative,
        census_geoid_state_senate,
        census_geoid_tract,
        census_geoid_zcta,
        p.year
    FROM spatial.parcel p
    LEFT JOIN distinct_joined dj
        ON p.year = dj.year
        AND p.x_3435 = dj.x_3435
        AND p.y_3435 = dj.y_3435
    WHERE p.year >= '2012'
)