-- CTAS to create a table of distance to the nearest CTA stop for each PIN
CREATE TABLE IF NOT EXISTS proximity.dist_pin_to_cta_stop
WITH (
    format='Parquet',
    write_compression = 'SNAPPY',
    external_location='s3://ccao-data-warehouse-us-east-1/proximity/dist_pin_to_cta_stop',
    partitioned_by = ARRAY['year'],
    bucketed_by = ARRAY['pin10'],
    bucket_count = 5
) AS (
    WITH pin_locations AS (
        SELECT DISTINCT x_3435, y_3435
        FROM spatial.parcel
        WHERE year >= (SELECT MIN(year) FROM spatial.transit_stop)
    ),
    stop_locations AS (
        SELECT *
        FROM spatial.transit_stop
        WHERE agency = 'cta'
        AND route_type = 1
    ),
    distances AS (
        SELECT
            p.x_3435,
            p.y_3435,
            o.stop_id,
            o.stop_name,
            o.year,
            ST_Distance(
                ST_Point(p.x_3435, p.y_3435),
                ST_GeomFromBinary(o.geometry_3435)
            ) distance
        FROM pin_locations p
        CROSS JOIN stop_locations o
    ),
    xy_to_stop_dist AS (
        SELECT
            d1.x_3435,
            d1.y_3435,
            d1.stop_id,
            d1.stop_name,
            d2.dist_ft,
            d1.year
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
        ARBITRARY(xy.stop_id) AS stop_id,
        ARBITRARY(xy.stop_name) AS stop_name,
        ARBITRARY(xy.dist_ft) AS dist_ft,
        p.year
    FROM spatial.parcel p
    INNER JOIN xy_to_stop_dist xy
        ON p.x_3435 = xy.x_3435
        AND p.y_3435 = xy.y_3435
        AND p.year = xy.year
    GROUP BY p.pin10, p.year
)