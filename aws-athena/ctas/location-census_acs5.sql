-- CTAS specifically to facilitate attribute joins of PRIOR YEAR ACS data to
-- current PINs. For example, 2020 ACS data is not yet release, but we do have
-- current (2021) geographies and PINs. We need to join 2019 geographies to
-- 2021 PINs to facilitate joining 2019 ACS5 data
CREATE TABLE IF NOT EXISTS location.census_acs5
WITH (
    FORMAT = 'Parquet',
    WRITE_COMPRESSION = 'SNAPPY',
    EXTERNAL_LOCATION = 's3://ccao-athena-ctas-us-east-1/location/census_acs5',
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
        FROM spatial.census
    ),

    acs5_max_year AS (
        SELECT MAX(year) AS max_year
        FROM census.acs5
    ),

    acs5_year_fill AS (
        SELECT
            dy.year AS pin_year,
            MAX(df.year) AS fill_year
        FROM census.acs5 AS df
        CROSS JOIN distinct_years AS dy
        WHERE dy.year >= df.year
        GROUP BY dy.year
    ),

    distinct_joined AS (
        SELECT
            p.x_3435,
            p.y_3435,
            MAX(CASE
                WHEN cen.geography = 'congressional_district' THEN cen.geoid
                ELSE NULL
            END) AS census_acs5_congressional_district_geoid,
            MAX(CASE
                WHEN cen.geography = 'county_subdivision' THEN cen.geoid
                ELSE NULL
            END) AS census_acs5_county_subdivision_geoid,
            MAX(CASE
                WHEN cen.geography = 'place' THEN cen.geoid
                ELSE NULL
            END) AS census_acs5_place_geoid,
            MAX(CASE
                WHEN cen.geography = 'puma' THEN cen.geoid
                ELSE NULL
            END) AS census_acs5_puma_geoid,
            MAX(CASE
                WHEN cen.geography = 'school_district_elementary' THEN cen.geoid
                ELSE NULL
            END) AS census_acs5_school_district_elementary_geoid,
            MAX(CASE
                WHEN cen.geography = 'school_district_secondary' THEN cen.geoid
                ELSE NULL
            END) AS census_acs5_school_district_secondary_geoid,
            MAX(CASE
                WHEN cen.geography = 'school_district_unified' THEN cen.geoid
                ELSE NULL
            END) AS census_acs5_school_district_unified_geoid,
            MAX(CASE
                WHEN cen.geography = 'state_representative' THEN cen.geoid
                ELSE NULL
            END) AS census_acs5_state_representative_geoid,
            MAX(CASE
                WHEN cen.geography = 'state_senate' THEN cen.geoid
                ELSE NULL
            END) AS census_acs5_state_senate_geoid,
            MAX(CASE
                WHEN cen.geography = 'tract' THEN cen.geoid
                ELSE NULL
            END) AS census_acs5_tract_geoid,
            cen.year
        FROM distinct_pins AS p
        LEFT JOIN (
            SELECT *
            FROM spatial.census
            WHERE year <= (SELECT max_year FROM acs5_max_year)
        ) AS cen
            ON ST_WITHIN(
                ST_POINT(p.x_3435, p.y_3435),
                ST_GEOMFROMBINARY(cen.geometry_3435)
            )
        GROUP BY p.x_3435, p.y_3435, cen.year
    )

    SELECT
        p.pin10,
        census_acs5_congressional_district_geoid,
        census_acs5_county_subdivision_geoid,
        census_acs5_place_geoid,
        census_acs5_puma_geoid,
        census_acs5_school_district_elementary_geoid,
        census_acs5_school_district_secondary_geoid,
        census_acs5_school_district_unified_geoid,
        census_acs5_state_representative_geoid,
        census_acs5_state_senate_geoid,
        census_acs5_tract_geoid,
        dj.year AS census_acs5_data_year,
        p.year
    FROM spatial.parcel AS p
    LEFT JOIN acs5_year_fill AS f
        ON p.year = f.pin_year
    LEFT JOIN distinct_joined AS dj
        ON f.fill_year = dj.year
        AND p.x_3435 = dj.x_3435
        AND p.y_3435 = dj.y_3435
    WHERE p.year >= (SELECT MIN(year) FROM distinct_years_rhs)
)