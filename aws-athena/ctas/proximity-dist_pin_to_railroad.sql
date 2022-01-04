-- CTAS to create a table of distance to the nearest rail tracks for each PIN
CREATE TABLE IF NOT EXISTS proximity.dist_pin_to_railroad
WITH (
    format='Parquet',
    write_compression = 'SNAPPY',
    external_location='s3://ccao-athena-ctas-us-east-1/proximity/dist_pin_to_railroad',
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
    railroad_location AS (
        SELECT fill_years.pin_year, fill_data.*
        FROM (
            SELECT dy.year AS pin_year, MAX(df.year) AS fill_year
            FROM spatial.railroad df
            CROSS JOIN distinct_years dy
            WHERE dy.year >= df.year
            GROUP BY dy.year
        ) fill_years
        LEFT JOIN spatial.railroad fill_data
            ON fill_years.fill_year = fill_data.year
    ),
    distances AS (
        SELECT
            p.x_3435,
            p.y_3435,
            o.name_id,
            o.name_anno,
            o.pin_year,
            o.year,
            ST_Distance(
                ST_Point(p.x_3435, p.y_3435),
                ST_GeomFromBinary(o.geometry_3435)
            ) distance
        FROM distinct_pins p
        CROSS JOIN railroad_location o
    ),
    xy_to_railroad_dist AS (
        SELECT
            d1.x_3435,
            d1.y_3435,
            d1.name_id,
            d1.name_anno,
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
        ARBITRARY(xy.name_id) AS nearest_railroad_id,
        ARBITRARY(xy.name_anno) AS nearest_railroad_name,
        ARBITRARY(xy.dist_ft) AS nearest_railroad_dist_ft,
        ARBITRARY(xy.year) AS nearest_railroad_data_year,
        p.year
    FROM spatial.parcel p
    INNER JOIN xy_to_railroad_dist xy
        ON p.x_3435 = xy.x_3435
        AND p.y_3435 = xy.y_3435
        AND p.year = xy.pin_year
    GROUP BY p.pin10, p.year
)