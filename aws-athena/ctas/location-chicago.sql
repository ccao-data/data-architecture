CREATE TABLE IF NOT EXISTS location.chicago
WITH (
    format='Parquet',
    write_compression = 'SNAPPY',
    external_location='s3://ccao-athena-ctas-us-east-1/location/chicago',
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
        SELECT DISTINCT year FROM spatial.ward_chicago
        UNION ALL
        SELECT DISTINCT year FROM spatial.police_district
        UNION ALL
        SELECT DISTINCT year FROM spatial.community_area
        UNION ALL
        SELECT DISTINCT year FROM spatial.industrial_corridor
    ),
    ward AS (
        SELECT
            p.x_3435, p.y_3435,
            MAX(CAST(CAST(cprod.ward_chicago_num AS integer) AS varchar)) AS chicago_ward_num,
            MAX(cprod.year) AS chicago_ward_data_year,
            cprod.pin_year
        FROM distinct_pins p
        LEFT JOIN (
            SELECT fill_years.pin_year, fill_data.*
            FROM (
                SELECT dy.year AS pin_year, MAX(df.year) AS fill_year
                FROM spatial.ward_chicago df
                CROSS JOIN distinct_years dy
                WHERE dy.year >= df.year
                GROUP BY dy.year
            ) fill_years
            LEFT JOIN spatial.ward_chicago fill_data
                ON fill_years.fill_year = fill_data.year
        ) cprod
        ON ST_Within(ST_Point(p.x_3435, p.y_3435), ST_GeomFromBinary(cprod.geometry_3435))
        GROUP BY p.x_3435, p.y_3435, cprod.pin_year
    ),
    police_district AS (
        SELECT
            p.x_3435, p.y_3435,
            MAX(CAST(CAST(cprod.pd_num AS integer) AS varchar)) AS chicago_police_district_num,
            MAX(cprod.year) AS chicago_police_district_data_year,
            cprod.pin_year
        FROM distinct_pins p
        LEFT JOIN (
            SELECT fill_years.pin_year, fill_data.*
            FROM (
                SELECT dy.year AS pin_year, MAX(df.year) AS fill_year
                FROM spatial.police_district df
                CROSS JOIN distinct_years dy
                WHERE dy.year >= df.year
                GROUP BY dy.year
            ) fill_years
            LEFT JOIN spatial.police_district fill_data
                ON fill_years.fill_year = fill_data.year
        ) cprod
        ON ST_Within(ST_Point(p.x_3435, p.y_3435), ST_GeomFromBinary(cprod.geometry_3435))
        GROUP BY p.x_3435, p.y_3435, cprod.pin_year
    ),
    community_area AS (
        SELECT
            p.x_3435, p.y_3435,
            MAX(CAST(CAST(cprod.area_number AS integer) AS varchar)) AS chicago_community_area_num,
            MAX(cprod.community) AS chicago_community_area_name,
            MAX(cprod.year) AS chicago_community_area_data_year,
            cprod.pin_year
        FROM distinct_pins p
        LEFT JOIN (
            SELECT fill_years.pin_year, fill_data.*
            FROM (
                SELECT dy.year AS pin_year, MAX(df.year) AS fill_year
                FROM spatial.community_area df
                CROSS JOIN distinct_years dy
                WHERE dy.year >= df.year
                GROUP BY dy.year
            ) fill_years
            LEFT JOIN spatial.community_area fill_data
                ON fill_years.fill_year = fill_data.year
        ) cprod
        ON ST_Within(ST_Point(p.x_3435, p.y_3435), ST_GeomFromBinary(cprod.geometry_3435))
        GROUP BY p.x_3435, p.y_3435, cprod.pin_year
    ),
    industrial_corridor AS (
        SELECT
            p.x_3435, p.y_3435,
            MAX(CAST(CAST(cprod.num AS integer) AS varchar)) AS chicago_industrial_corridor_num,
            MAX(cprod.name) AS chicago_industrial_corridor_name,
            MAX(cprod.year) AS chicago_industrial_corridor_data_year,
            cprod.pin_year
        FROM distinct_pins p
        LEFT JOIN (
            SELECT fill_years.pin_year, fill_data.*
            FROM (
                SELECT dy.year AS pin_year, MAX(df.year) AS fill_year
                FROM spatial.industrial_corridor df
                CROSS JOIN distinct_years dy
                WHERE dy.year >= df.year
                GROUP BY dy.year
            ) fill_years
            LEFT JOIN spatial.industrial_corridor fill_data
                ON fill_years.fill_year = fill_data.year
        ) cprod
        ON ST_Within(ST_Point(p.x_3435, p.y_3435), ST_GeomFromBinary(cprod.geometry_3435))
        GROUP BY p.x_3435, p.y_3435, cprod.pin_year
    )
    SELECT
        p.pin10,
        chicago_ward_num,
        chicago_ward_data_year,
        chicago_community_area_num,
        chicago_community_area_name,
        chicago_community_area_data_year,
        chicago_industrial_corridor_num,
        chicago_industrial_corridor_name,
        chicago_industrial_corridor_data_year,
        chicago_police_district_num,
        chicago_police_district_data_year,
        p.year
    FROM spatial.parcel p
    LEFT JOIN ward
        ON p.x_3435 = ward.x_3435
        AND p.y_3435 = ward.y_3435
        AND p.year = ward.pin_year
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