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
    WITH distances AS (
        SELECT
            p.pin10,
            p.year,
            o.stop_id,
            o.stop_name,
            ST_Distance(
                ST_Point(p.x_3435, p.y_3435),
                ST_GeomFromBinary(o.geometry_3435)
            ) distance
        FROM spatial.parcel p
        INNER JOIN (
            SELECT *
            FROM spatial.transit_stop
            WHERE agency = 'cta'
            AND route_type = 1
        ) o ON p.year = o.year
    )
    -- The ARBITRARY() function is used to pick a random stop from the ones that
    -- are the same distance i.e. they have the identical lat/lon
    SELECT
        d1.pin10,
        ARBITRARY(d1.stop_id) AS stop_id,
        ARBITRARY(d1.stop_name) AS stop_name,
        ARBITRARY(d2.min_dist) AS dist_ft,
        d1.year
    FROM distances d1
    INNER JOIN (
        SELECT
            pin10,
            year,
            MIN(distance) AS min_dist
        FROM distances
        GROUP BY pin10, year
    ) d2
        ON d1.pin10 = d2.pin10
        AND d1.year = d2.year
        AND d1.distance = d2.min_dist
    GROUP BY d1.year, d1.pin10
);