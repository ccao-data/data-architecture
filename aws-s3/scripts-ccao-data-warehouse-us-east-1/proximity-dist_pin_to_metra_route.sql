-- CTAS to create a table of distance to the nearest Metra route for each PIN
CREATE TABLE IF NOT EXISTS proximity.dist_pin_to_metra_route
WITH (
    format='Parquet',
    write_compression = 'SNAPPY',
    external_location='s3://ccao-data-warehouse-us-east-1/proximity/dist_pin_to_metra_route',
    partitioned_by = ARRAY['year'],
    bucketed_by = ARRAY['pin10'],
    bucket_count = 5
) AS (
    WITH pin_locations AS (
        SELECT DISTINCT x_3435, y_3435
        FROM spatial.parcel
        WHERE year >= (SELECT MIN(year) FROM spatial.transit_stop)
    ),
    route_locations AS (
        SELECT *
        FROM spatial.transit_route
        WHERE agency = 'metra'
        AND route_type = 2
    ),
    distances AS (
        SELECT
            p.x_3435,
            p.y_3435,
            o.route_id,
            o.route_long_name,
            o.year,
            ST_Distance(
                ST_Point(p.x_3435, p.y_3435),
                ST_GeomFromBinary(o.geometry_3435)
            ) distance
        FROM pin_locations p
        CROSS JOIN route_locations o
    ),
    xy_to_route_dist AS (
        SELECT
            d1.x_3435,
            d1.y_3435,
            d1.route_id,
            d1.route_long_name,
            d2.dist_ft,
            d1.year
        FROM distances d1
        INNER JOIN (
            SELECT
                x_3435,
                y_3435,
                year,
                MIN(distance) AS dist_ft
            FROM distances
            GROUP BY x_3435, y_3435, year
        ) d2
           ON d1.x_3435 = d2.x_3435
           AND d1.y_3435 = d2.y_3435
           AND d1.year = d2.year
           AND d1.distance = d2.dist_ft
    )
    SELECT
        p.pin10,
        ARBITRARY(xy.route_id) AS route_id,
        ARBITRARY(xy.route_long_name) AS route_long_name,
        ARBITRARY(xy.dist_ft) AS dist_ft,
        p.year
    FROM spatial.parcel p
    INNER JOIN xy_to_route_dist xy
        ON p.x_3435 = xy.x_3435
        AND p.y_3435 = xy.y_3435
        AND p.year = xy.year
    GROUP BY p.pin10, p.year
)