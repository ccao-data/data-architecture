-- CTAS to create a table of distance to the nearest cemetery for each PIN
CREATE TABLE IF NOT EXISTS proximity.dist_pin_to_cemetery
WITH (
    format='Parquet',
    write_compression = 'SNAPPY',
    external_location='s3://ccao-athena-ctas-us-east-1/proximity/dist_pin_to_cemetery',
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
    cemetery_location AS (
        SELECT fill_years.pin_year, fill_data.*
        FROM (
            SELECT dy.year AS pin_year, MAX(df.year) AS fill_year
            FROM spatial.cemetery df
            CROSS JOIN distinct_years dy
            WHERE dy.year >= df.year
            GROUP BY dy.year
        ) fill_years
        LEFT JOIN spatial.cemetery fill_data
            ON fill_years.fill_year = fill_data.year
    ),
    distances AS (
        SELECT
            p.x_3435,
            p.y_3435,
            o.name,
            o.gniscode,
            o.pin_year,
            o.year,
            ST_Distance(
                ST_Point(p.x_3435, p.y_3435),
                ST_GeomFromBinary(o.geometry_3435)
            ) distance
        FROM distinct_pins p
        CROSS JOIN cemetery_location o
    ),
    xy_to_cemetery_dist AS (
        SELECT
            d1.x_3435,
            d1.y_3435,
            d1.name,
            d1.gniscode,
            d1.pin_year,
            d1.year,
            d2.dist_ft
        FROM distances d1
        INNER JOIN (
            SELECT
                x_3435,
                y_3435,
                pin_year,
                MIN(distance) AS dist_ft
            FROM distances
            GROUP BY x_3435, y_3435, pin_year
        ) d2
           ON d1.x_3435 = d2.x_3435
           AND d1.y_3435 = d2.y_3435
           AND d1.pin_year = d2.pin_year
           AND d1.distance = d2.dist_ft
    )
    SELECT
        p.pin10,
        CAST(CAST(ARBITRARY(xy.gniscode) AS bigint) AS varchar) AS nearest_cemetery_gnis_code,
        ARBITRARY(xy.name) AS nearest_cemetery_name,
        ARBITRARY(xy.dist_ft) AS nearest_cemetery_dist_ft,
        ARBITRARY(xy.year) AS nearest_cemetery_data_year,
        p.year
    FROM spatial.parcel p
    INNER JOIN xy_to_cemetery_dist xy
        ON p.x_3435 = xy.x_3435
        AND p.y_3435 = xy.y_3435
        AND p.year = xy.pin_year
    GROUP BY p.pin10, p.year
)