-- CTAS to create a table of distance to the nearest bike trail for each PIN
CREATE TABLE IF NOT EXISTS proximity.dist_pin_to_bike_trail
WITH (
    format='Parquet',
    write_compression = 'SNAPPY',
    external_location='s3://ccao-athena-ctas-us-east-1/proximity/dist_pin_to_bike_trail',
    partitioned_by = ARRAY['year']
) AS (
    WITH pin_locations AS (
        SELECT DISTINCT x_3435, y_3435
        FROM spatial.parcel
        WHERE year >= (SELECT MIN(year) FROM spatial.transit_stop)
    ),
    bike_trail_location AS (
        SELECT *
        FROM spatial.bike_trail
    ),
    distances AS (
        SELECT
            p.x_3435,
            p.y_3435,
            o.name,
            o.street,
            ST_Distance(
                ST_Point(p.x_3435, p.y_3435),
                ST_GeomFromBinary(o.geometry_3435)
            ) distance
        FROM pin_locations p
        CROSS JOIN bike_trail_location o
    ),
    xy_to_bike_trail_dist AS (
        SELECT
            d1.x_3435,
            d1.y_3435,
            d1.name,
            d1.street,
            d2.dist_ft
        FROM distances d1
        INNER JOIN (
            SELECT
                x_3435,
                y_3435,
                MIN(distance) AS dist_ft
            FROM distances
            GROUP BY x_3435, y_3435
        ) d2
           ON d1.x_3435 = d2.x_3435
           AND d1.y_3435 = d2.y_3435
           AND d1.distance = d2.dist_ft
    )
    SELECT
        p.pin10,
        ARBITRARY(xy.name) AS name,
        ARBITRARY(xy.street) AS gnis_code,
        ARBITRARY(xy.dist_ft) AS dist_ft,
        p.year
    FROM spatial.parcel p
    INNER JOIN xy_to_bike_trail_dist xy
        ON p.x_3435 = xy.x_3435
        AND p.y_3435 = xy.y_3435
    WHERE p.year >= (SELECT MIN(year) FROM spatial.transit_stop)
    GROUP BY p.pin10, p.year
)