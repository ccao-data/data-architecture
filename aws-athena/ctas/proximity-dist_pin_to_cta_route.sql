-- CTAS to create a table of distance to the nearest CTA route for each PIN
CREATE TABLE IF NOT EXISTS proximity.dist_pin_to_cta_route
WITH (
    FORMAT = 'Parquet',
    WRITE_COMPRESSION = 'SNAPPY',
    EXTERNAL_LOCATION
    = 's3://ccao-athena-ctas-us-east-1/proximity/dist_pin_to_cta_route',
    PARTITIONED_BY = ARRAY['year'],
    BUCKETED_BY = ARRAY['pin10'],
    BUCKET_COUNT = 1
) AS (
    WITH distinct_pins AS (
        SELECT DISTINCT
            x_3435,
            y_3435
        FROM spatial.parcel
    ),

    distinct_years AS (
        SELECT DISTINCT year
        FROM spatial.parcel
    ),

    cta_route_location AS (
        SELECT
            fill_years.pin_year,
            fill_data.*
        FROM (
            SELECT
                dy.year AS pin_year,
                MAX(df.year) AS fill_year
            FROM spatial.transit_route AS df
            CROSS JOIN distinct_years AS dy
            WHERE dy.year >= df.year
            GROUP BY dy.year
        ) AS fill_years
        LEFT JOIN spatial.transit_route AS fill_data
            ON fill_years.fill_year = fill_data.year
        WHERE agency = 'cta'
            AND route_type = 1
    ),

    distances AS (
        SELECT
            p.x_3435,
            p.y_3435,
            o.route_id,
            o.route_long_name,
            o.pin_year,
            o.year,
            ST_DISTANCE(
                ST_POINT(p.x_3435, p.y_3435),
                ST_GEOMFROMBINARY(o.geometry_3435)
            ) AS distance
        FROM distinct_pins AS p
        CROSS JOIN cta_route_location AS o
    ),

    xy_to_cta_route_dist AS (
        SELECT
            d1.x_3435,
            d1.y_3435,
            d1.route_id,
            d1.route_long_name,
            d1.pin_year,
            d1.year,
            d2.dist_ft
        FROM distances AS d1
        INNER JOIN (
            SELECT
                x_3435,
                y_3435,
                pin_year,
                MIN(distance) AS dist_ft
            FROM distances
            GROUP BY x_3435, y_3435, pin_year
        ) AS d2
            ON d1.x_3435 = d2.x_3435
            AND d1.y_3435 = d2.y_3435
            AND d1.pin_year = d2.pin_year
            AND d1.distance = d2.dist_ft
    )

    SELECT
        p.pin10,
        ARBITRARY(xy.route_id) AS nearest_cta_route_id,
        ARBITRARY(xy.route_long_name) AS nearest_cta_route_name,
        ARBITRARY(xy.dist_ft) AS nearest_cta_route_dist_ft,
        ARBITRARY(xy.year) AS nearest_cta_route_data_year,
        p.year
    FROM spatial.parcel AS p
    INNER JOIN xy_to_cta_route_dist AS xy
        ON p.x_3435 = xy.x_3435
        AND p.y_3435 = xy.y_3435
        AND p.year = xy.pin_year
    GROUP BY p.pin10, p.year
)