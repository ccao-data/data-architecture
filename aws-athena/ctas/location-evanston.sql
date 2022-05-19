CREATE TABLE IF NOT EXISTS location.evanston
WITH (
    format='Parquet',
    write_compression = 'SNAPPY',
    external_location='s3://ccao-athena-ctas-us-east-1/location/evanston',
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
        SELECT DISTINCT year FROM spatial.ward_evanston
    ),
    ward AS (
        SELECT
            p.x_3435, p.y_3435,
            MAX(CAST(CAST(cprod.ward_evanston_num AS integer) AS varchar)) AS evanston_ward_num,
            MAX(cprod.year) AS evanston_ward_data_year,
            cprod.pin_year
        FROM distinct_pins p
        LEFT JOIN (
            SELECT fill_years.pin_year, fill_data.*
            FROM (
                SELECT dy.year AS pin_year, MAX(df.year) AS fill_year
                FROM spatial.ward_evanston df
                CROSS JOIN distinct_years dy
                WHERE dy.year >= df.year
                GROUP BY dy.year
            ) fill_years
            LEFT JOIN spatial.ward_evanston fill_data
                ON fill_years.fill_year = fill_data.year
        ) cprod
        ON ST_Within(ST_Point(p.x_3435, p.y_3435), ST_GeomFromBinary(cprod.geometry_3435))
        GROUP BY p.x_3435, p.y_3435, cprod.pin_year
    )
    SELECT
        p.pin10,
        evanston_ward_num,
        evanston_ward_data_year,
        p.year
    FROM spatial.parcel p
    LEFT JOIN ward
        ON p.x_3435 = ward.x_3435
        AND p.y_3435 = ward.y_3435
        AND p.year = ward.pin_year
    WHERE p.year >= (SELECT MIN(year) FROM distinct_years_rhs)
)