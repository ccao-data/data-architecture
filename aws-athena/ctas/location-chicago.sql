CREATE TABLE IF NOT EXISTS location.chicago
WITH (
    format='Parquet',
    write_compression = 'SNAPPY',
    external_location='s3://ccao-athena-ctas-us-east-1/location/chicago',
    partitioned_by = ARRAY['year']
) AS (
    WITH distinct_pins AS (
        SELECT DISTINCT x_3435, y_3435
        FROM spatial.parcel
        WHERE year >= '2012'
    ),
    distinct_years AS (
        SELECT DISTINCT year
        FROM spatial.parcel
        WHERE year >= '2012'
    ),
    ward AS (
        SELECT
            p.x_3435, p.y_3435,
            MAX(CAST(CAST(cprod.ward_num AS integer) AS varchar)) AS chicago_ward_num,
            MAX(cprod.year) AS chicago_ward_data_year,
            cprod.pin_year
        FROM distinct_pins p
        LEFT JOIN (
            SELECT fill_years.pin_year, fill_data.*
            FROM (
                SELECT dy.year AS pin_year, MAX(df.year) AS fill_year
                FROM spatial.ward df
                CROSS JOIN distinct_years dy
                WHERE dy.year >= df.year
                GROUP BY dy.year
            ) fill_years
            LEFT JOIN spatial.ward fill_data
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
    other AS (
        SELECT
            p.x_3435, p.y_3435,
            MAX(CAST(CAST(comm_area.area_number AS integer) AS varchar)) AS chicago_community_area_num,
            MAX(comm_area.community) AS chicago_community_area_name,
            MAX(CAST(CAST(ind_corr.num AS integer) AS varchar)) AS chicago_industrial_corridor_num,
            MAX(ind_corr.name) AS chicago_industrial_corridor_name
        FROM distinct_pins p
        LEFT JOIN spatial.community_area comm_area
            ON ST_Within(ST_Point(p.x_3435, p.y_3435), ST_GeomFromBinary(comm_area.geometry_3435))
        LEFT JOIN spatial.industrial_corridor ind_corr
            ON ST_Within(ST_Point(p.x_3435, p.y_3435), ST_GeomFromBinary(ind_corr.geometry_3435))
        GROUP BY p.x_3435, p.y_3435
    )
    SELECT
        p.pin10,
        ward.chicago_ward_num,
        ward.chicago_ward_data_year,
        other.chicago_community_area_num,
        other.chicago_community_area_name,
        CASE
            WHEN other.chicago_community_area_num IS NOT NULL THEN '2018'
            ELSE NULL END AS chicago_community_area_data_year,
        other.chicago_industrial_corridor_num,
        other.chicago_industrial_corridor_name,
        CASE
            WHEN other.chicago_industrial_corridor_num IS NOT NULL THEN '2013'
            ELSE NULL END AS chicago_industrial_corridor_data_year,
        police_district.chicago_police_district_num,
        police_district.chicago_police_district_data_year,
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
    LEFT JOIN other
        ON p.x_3435 = other.x_3435
        AND p.y_3435 = other.y_3435
    WHERE p.year >= '2012'
)