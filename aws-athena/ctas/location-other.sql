CREATE TABLE IF NOT EXISTS location.other
WITH (
    format='Parquet',
    write_compression = 'SNAPPY',
    external_location='s3://ccao-athena-ctas-us-east-1/location/other',
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
        SELECT DISTINCT '2021' AS year FROM spatial.subdivision
        UNION ALL
        SELECT DISTINCT '2014' AS year FROM spatial.enterprise_zone
    ),
    subdivision AS (
        SELECT
            p.x_3435, p.y_3435,
            MAX(cprod.pagesubref) AS misc_subdivision_id,
            MAX(cprod.year) AS misc_subdivision_data_year,
            cprod.pin_year
        FROM distinct_pins p
        LEFT JOIN (
            SELECT fill_years.pin_year, fill_data.*
            FROM (
                SELECT dy.year AS pin_year, MAX(df.year) AS fill_year
                FROM spatial.subdivision df
                CROSS JOIN distinct_years dy
                WHERE dy.year >= df.year
                GROUP BY dy.year
            ) fill_years
            LEFT JOIN spatial.subdivision fill_data
                ON fill_years.fill_year = fill_data.year
        ) cprod
        ON ST_Within(ST_Point(p.x_3435, p.y_3435), ST_GeomFromBinary(cprod.geometry_3435))
        GROUP BY p.x_3435, p.y_3435, cprod.pin_year
    )
    SELECT
        p.pin10,
        misc_subdivision_id,
        misc_subdivision_data_year,
        p.year
    FROM spatial.parcel p
    LEFT JOIN subdivision
        ON p.x_3435 = subdivision.x_3435
        AND p.y_3435 = subdivision.y_3435
        AND p.year = subdivision.pin_year
    WHERE p.year >= (SELECT MIN(year) FROM distinct_years_rhs)
)