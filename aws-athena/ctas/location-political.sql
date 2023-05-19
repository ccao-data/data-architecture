CREATE TABLE IF NOT EXISTS location.political
WITH (
    format='Parquet',
    write_compression = 'SNAPPY',
    external_location='s3://ccao-athena-ctas-us-east-1/location/political',
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
        SELECT DISTINCT year FROM spatial.board_of_review_district
        UNION ALL
        SELECT DISTINCT year FROM spatial.commissioner_district
        UNION ALL
        SELECT DISTINCT year FROM spatial.judicial_district
        UNION ALL
        SELECT DISTINCT year FROM spatial.ward
    ),
    board_of_review_district AS (
        SELECT
            p.x_3435, p.y_3435,
            MAX(CAST(CAST(cprod.board_of_review_district_num AS integer) AS varchar)) AS cook_board_of_review_district_num,
            MAX(cprod.year) AS cook_board_of_review_district_data_year,
            cprod.pin_year
        FROM distinct_pins p
        LEFT JOIN (
            SELECT fill_years.pin_year, fill_data.*
            FROM (
                SELECT dy.year AS pin_year, MAX(df.year) AS fill_year
                FROM spatial.board_of_review_district df
                CROSS JOIN distinct_years dy
                WHERE dy.year >= df.year
                GROUP BY dy.year
            ) fill_years
            LEFT JOIN spatial.board_of_review_district fill_data
                ON fill_years.fill_year = fill_data.year
        ) cprod
        ON ST_Within(ST_Point(p.x_3435, p.y_3435), ST_GeomFromBinary(cprod.geometry_3435))
        GROUP BY p.x_3435, p.y_3435, cprod.pin_year
    ),
    commissioner_district AS (
        SELECT
            p.x_3435, p.y_3435,
            MAX(CAST(CAST(cprod.commissioner_district_num AS integer) AS varchar)) AS cook_commissioner_district_num,
            MAX(cprod.year) AS cook_commissioner_district_data_year,
            cprod.pin_year
        FROM distinct_pins p
        LEFT JOIN (
            SELECT fill_years.pin_year, fill_data.*
            FROM (
                SELECT dy.year AS pin_year, MAX(df.year) AS fill_year
                FROM spatial.commissioner_district df
                CROSS JOIN distinct_years dy
                WHERE dy.year >= df.year
                GROUP BY dy.year
            ) fill_years
            LEFT JOIN spatial.commissioner_district fill_data
                ON fill_years.fill_year = fill_data.year
        ) cprod
        ON ST_Within(ST_Point(p.x_3435, p.y_3435), ST_GeomFromBinary(cprod.geometry_3435))
        GROUP BY p.x_3435, p.y_3435, cprod.pin_year
    ),
    judicial_district AS (
        SELECT
            p.x_3435, p.y_3435,
            MAX(CAST(CAST(cprod.judicial_district_num AS integer) AS varchar)) AS cook_judicial_district_num,
            MAX(cprod.year) AS cook_judicial_district_data_year,
            cprod.pin_year
        FROM distinct_pins p
        LEFT JOIN (
            SELECT fill_years.pin_year, fill_data.*
            FROM (
                SELECT dy.year AS pin_year, MAX(df.year) AS fill_year
                FROM spatial.judicial_district df
                CROSS JOIN distinct_years dy
                WHERE dy.year >= df.year
                GROUP BY dy.year
            ) fill_years
            LEFT JOIN spatial.judicial_district fill_data
                ON fill_years.fill_year = fill_data.year
        ) cprod
        ON ST_Within(ST_Point(p.x_3435, p.y_3435), ST_GeomFromBinary(cprod.geometry_3435))
        GROUP BY p.x_3435, p.y_3435, cprod.pin_year
    ),
    ward_chicago AS (
        SELECT
            p.x_3435, p.y_3435,
            MAX(cprod.ward_num) AS ward_num,
            MAX(cprod.ward_name) AS ward_name,
            MAX(
                CASE WHEN SUBSTR(cprod.ward_name, 1, 1) = 'c' THEN cprod.year ELSE NULL END
                ) AS ward_chicago_data_year,
            cprod.pin_year
        FROM distinct_pins p
        LEFT JOIN (
            SELECT fill_years.pin_year, fill_data.*
            FROM (
                SELECT dy.year AS pin_year,
                MAX(
                    CASE WHEN SUBSTR(df.ward_name, 1, 1) = 'c' THEN df.year ELSE NULL END
                    ) AS fill_year
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
    ward_evanston AS (
        SELECT
            p.x_3435, p.y_3435,
            MAX(cprod.ward_num) AS ward_num,
            MAX(cprod.ward_name) AS ward_name,
            MAX(
                CASE WHEN SUBSTR(cprod.ward_name, 1, 1) = 'e' THEN cprod.year ELSE NULL END
                ) AS ward_evanston_data_year,
            cprod.pin_year
        FROM distinct_pins p
        LEFT JOIN (
            SELECT fill_years.pin_year, fill_data.*
            FROM (
                SELECT dy.year AS pin_year,
                MAX(
                    CASE WHEN SUBSTR(df.ward_name, 1, 1) = 'e' THEN df.year ELSE NULL END
                    ) AS fill_year
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
    )
    SELECT
        p.pin10,
        cook_board_of_review_district_num,
        cook_board_of_review_district_data_year,
        cook_commissioner_district_num,
        cook_commissioner_district_data_year,
        cook_judicial_district_num,
        cook_judicial_district_data_year,
        CASE
            WHEN ward_evanston.ward_num IS NOT NULL THEN ward_evanston.ward_num
            ELSE ward_chicago.ward_num
        END AS ward_num,
        CASE
            WHEN ward_evanston.ward_name IS NOT NULL THEN ward_evanston.ward_name
            ELSE ward_chicago.ward_name
        END AS ward_name,
        ward_chicago_data_year,
        ward_evanston_data_year,
        p.year
    FROM spatial.parcel p
    LEFT JOIN board_of_review_district
        ON p.x_3435 = board_of_review_district.x_3435
        AND p.y_3435 = board_of_review_district.y_3435
        AND p.year = board_of_review_district.pin_year
    LEFT JOIN commissioner_district
        ON p.x_3435 = commissioner_district.x_3435
        AND p.y_3435 = commissioner_district.y_3435
        AND p.year = commissioner_district.pin_year
    LEFT JOIN judicial_district
        ON p.x_3435 = judicial_district.x_3435
        AND p.y_3435 = judicial_district.y_3435
        AND p.year = judicial_district.pin_year
    LEFT JOIN ward_chicago
        ON p.x_3435 = ward_chicago.x_3435
        AND p.y_3435 = ward_chicago.y_3435
        AND p.year = ward_chicago.pin_year
    LEFT JOIN ward_evanston
        ON p.x_3435 = ward_evanston.x_3435
        AND p.y_3435 = ward_evanston.y_3435
        AND p.year = ward_evanston.pin_year
    WHERE p.year >= (SELECT MIN(year) FROM distinct_years_rhs)
)