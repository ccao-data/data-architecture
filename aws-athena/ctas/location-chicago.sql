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
            MAX(CAST(CAST(cprod.ward_num AS integer) AS varchar)) AS chicago_num_ward,
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
            MAX(CAST(CAST(cprod.pd_num AS integer) AS varchar)) AS chicago_num_police_district,
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
            MAX(CAST(CAST(comm_area.area_number AS integer) AS varchar)) AS chicago_num_community_area,
            MAX(comm_area.community) AS chicago_name_community_area,
            MAX(CAST(CAST(ind_corr.num AS integer) AS varchar)) AS chicago_num_industrial_corridor,
            MAX(ind_corr.name) AS chicago_name_industrial_corridor
        FROM distinct_pins p
        LEFT JOIN spatial.community_area comm_area
            ON ST_Within(ST_Point(p.x_3435, p.y_3435), ST_GeomFromBinary(comm_area.geometry_3435))
        LEFT JOIN spatial.industrial_corridor ind_corr
            ON ST_Within(ST_Point(p.x_3435, p.y_3435), ST_GeomFromBinary(ind_corr.geometry_3435))
        GROUP BY p.x_3435, p.y_3435
    )
    SELECT
        p.pin10,
        ward.chicago_num_ward,
        other.chicago_num_community_area,
        other.chicago_name_community_area,
        other.chicago_num_industrial_corridor,
        other.chicago_name_industrial_corridor,
        police_district.chicago_num_police_district,
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