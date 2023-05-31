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
            dp.x_3435,
            dp.y_3435,
            MAX(CAST(CAST(cprod.pd_num AS INTEGER) AS VARCHAR))
                AS chicago_police_district_num,
            MAX(cprod.year) AS chicago_police_district_data_year,
            cprod.pin_year
        FROM distinct_pins AS dp
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
                ST_POINT(dp.x_3435, dp.y_3435),
                ST_GEOMFROMBINARY(cprod.geometry_3435)
            )
        GROUP BY dp.x_3435, dp.y_3435, cprod.pin_year
    ),

    community_area AS (
        SELECT
            dp.x_3435,
            dp.y_3435,
            MAX(CAST(CAST(cprod.area_number AS INTEGER) AS VARCHAR))
                AS chicago_community_area_num,
            MAX(cprod.community) AS chicago_community_area_name,
            MAX(cprod.year) AS chicago_community_area_data_year,
            cprod.pin_year
        FROM distinct_pins AS dp
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
                ST_POINT(dp.x_3435, dp.y_3435),
                ST_GEOMFROMBINARY(cprod.geometry_3435)
            )
        GROUP BY dp.x_3435, dp.y_3435, cprod.pin_year
    ),

    industrial_corridor AS (
        SELECT
            dp.x_3435,
            dp.y_3435,
            MAX(CAST(CAST(cprod.num AS INTEGER) AS VARCHAR))
                AS chicago_industrial_corridor_num,
            MAX(cprod.name) AS chicago_industrial_corridor_name,
            MAX(cprod.year) AS chicago_industrial_corridor_data_year,
            cprod.pin_year
        FROM distinct_pins AS dp
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
                ST_POINT(dp.x_3435, dp.y_3435),
                ST_GEOMFROMBINARY(cprod.geometry_3435)
            )
        GROUP BY dp.x_3435, dp.y_3435, cprod.pin_year
    )

    SELECT
        pcl.pin10,
        ca.chicago_community_area_num,
        ca.chicago_community_area_name,
        ca.chicago_community_area_data_year,
        ic.chicago_industrial_corridor_num,
        ic.chicago_industrial_corridor_name,
        ic.chicago_industrial_corridor_data_year,
        pd.chicago_police_district_num,
        pd.chicago_police_district_data_year,
        pcl.year
    FROM spatial.parcel AS pcl
    LEFT JOIN police_district AS pd
        ON pcl.x_3435 = pd.x_3435
        AND pcl.y_3435 = pd.y_3435
        AND pcl.year = pd.pin_year
    LEFT JOIN community_area AS ca
        ON pcl.x_3435 = ca.x_3435
        AND pcl.y_3435 = ca.y_3435
        AND pcl.year = ca.pin_year
    LEFT JOIN industrial_corridor AS ic
        ON pcl.x_3435 = ic.x_3435
        AND pcl.y_3435 = ic.y_3435
        AND pcl.year = ic.pin_year
    WHERE pcl.year >= (SELECT MIN(year) FROM distinct_years_rhs)
)
