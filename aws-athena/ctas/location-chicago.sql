CREATE TABLE IF NOT EXISTS location.chicago
WITH (
    FORMAT = 'Parquet',
    WRITE_COMPRESSION = 'SNAPPY',
    EXTERNAL_LOCATION = 's3://ccao-athena-ctas-us-east-1/location/chicago',
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
        SELECT DISTINCT year FROM spatial.police_district
        UNION ALL
        SELECT DISTINCT year FROM spatial.community_area
        UNION ALL
        SELECT DISTINCT year FROM spatial.industrial_corridor
    ),

    police_district AS (
        SELECT
            p.x_3435,
            p.y_3435,
            MAX(CAST(CAST(cprod.pd_num AS INTEGER) AS VARCHAR))
                AS chicago_police_district_num,
            MAX(cprod.year) AS chicago_police_district_data_year,
            cprod.pin_year
        FROM distinct_pins AS p
        LEFT JOIN (
            SELECT
                fill_years.pin_year,
                fill_data.*
            FROM (
                SELECT
                    dy.year AS pin_year,
                    MAX(df.year) AS fill_year
                FROM spatial.police_district AS df
                CROSS JOIN distinct_years AS dy
                WHERE dy.year >= df.year
                GROUP BY dy.year
            ) AS fill_years
            LEFT JOIN spatial.police_district AS fill_data
                ON fill_years.fill_year = fill_data.year
        ) AS cprod
            ON ST_WITHIN(
                ST_POINT(p.x_3435, p.y_3435),
                ST_GEOMFROMBINARY(cprod.geometry_3435)
            )
        GROUP BY p.x_3435, p.y_3435, cprod.pin_year
    ),

    community_area AS (
        SELECT
            p.x_3435,
            p.y_3435,
            MAX(CAST(CAST(cprod.area_number AS INTEGER) AS VARCHAR))
                AS chicago_community_area_num,
            MAX(cprod.community) AS chicago_community_area_name,
            MAX(cprod.year) AS chicago_community_area_data_year,
            cprod.pin_year
        FROM distinct_pins AS p
        LEFT JOIN (
            SELECT
                fill_years.pin_year,
                fill_data.*
            FROM (
                SELECT
                    dy.year AS pin_year,
                    MAX(df.year) AS fill_year
                FROM spatial.community_area AS df
                CROSS JOIN distinct_years AS dy
                WHERE dy.year >= df.year
                GROUP BY dy.year
            ) AS fill_years
            LEFT JOIN spatial.community_area AS fill_data
                ON fill_years.fill_year = fill_data.year
        ) AS cprod
            ON ST_WITHIN(
                ST_POINT(p.x_3435, p.y_3435),
                ST_GEOMFROMBINARY(cprod.geometry_3435)
            )
        GROUP BY p.x_3435, p.y_3435, cprod.pin_year
    ),

    industrial_corridor AS (
        SELECT
            p.x_3435,
            p.y_3435,
            MAX(CAST(CAST(cprod.num AS INTEGER) AS VARCHAR))
                AS chicago_industrial_corridor_num,
            MAX(cprod.name) AS chicago_industrial_corridor_name,
            MAX(cprod.year) AS chicago_industrial_corridor_data_year,
            cprod.pin_year
        FROM distinct_pins AS p
        LEFT JOIN (
            SELECT
                fill_years.pin_year,
                fill_data.*
            FROM (
                SELECT
                    dy.year AS pin_year,
                    MAX(df.year) AS fill_year
                FROM spatial.industrial_corridor AS df
                CROSS JOIN distinct_years AS dy
                WHERE dy.year >= df.year
                GROUP BY dy.year
            ) AS fill_years
            LEFT JOIN spatial.industrial_corridor AS fill_data
                ON fill_years.fill_year = fill_data.year
        ) AS cprod
            ON ST_WITHIN(
                ST_POINT(p.x_3435, p.y_3435),
                ST_GEOMFROMBINARY(cprod.geometry_3435)
            )
        GROUP BY p.x_3435, p.y_3435, cprod.pin_year
    )

    SELECT
        p.pin10,
        chicago_community_area_num,
        chicago_community_area_name,
        chicago_community_area_data_year,
        chicago_industrial_corridor_num,
        chicago_industrial_corridor_name,
        chicago_industrial_corridor_data_year,
        chicago_police_district_num,
        chicago_police_district_data_year,
        p.year
    FROM spatial.parcel AS p
    LEFT JOIN police_district
        ON p.x_3435 = police_district.x_3435
        AND p.y_3435 = police_district.y_3435
        AND p.year = police_district.pin_year
    LEFT JOIN community_area
        ON p.x_3435 = community_area.x_3435
        AND p.y_3435 = community_area.y_3435
        AND p.year = community_area.pin_year
    LEFT JOIN industrial_corridor
        ON p.x_3435 = industrial_corridor.x_3435
        AND p.y_3435 = industrial_corridor.y_3435
        AND p.year = industrial_corridor.pin_year
    WHERE p.year >= (SELECT MIN(year) FROM distinct_years_rhs)
)