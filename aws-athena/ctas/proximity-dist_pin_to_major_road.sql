-- CTAS to create a table of distance to the nearest major road for each PIN
CREATE TABLE IF NOT EXISTS proximity.dist_pin_to_major_road
WITH (
    FORMAT = 'Parquet',
    WRITE_COMPRESSION = 'SNAPPY',
    EXTERNAL_LOCATION
    = 's3://ccao-athena-ctas-us-east-1/proximity/dist_pin_to_major_road',
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

    major_road_location AS (
        SELECT
            fill_years.pin_year,
            fill_data.*
        FROM (
            SELECT
                dy.year AS pin_year,
                MAX(df.year) AS fill_year
            FROM spatial.major_road AS df
            CROSS JOIN distinct_years AS dy
            WHERE dy.year >= df.year
            GROUP BY dy.year
        ) AS fill_years
        LEFT JOIN spatial.major_road AS fill_data
            ON fill_years.fill_year = fill_data.year
    ),

    distances AS (
        SELECT
            dp.x_3435,
            dp.y_3435,
            loc.osm_id,
            loc.name,
            loc.pin_year,
            loc.year,
            ST_DISTANCE(
                ST_POINT(dp.x_3435, dp.y_3435),
                ST_GEOMFROMBINARY(loc.geometry_3435)
            ) AS distance
        FROM distinct_pins AS dp
        CROSS JOIN major_road_location AS loc
    ),

    xy_to_major_road_dist AS (
        SELECT
            d1.x_3435,
            d1.y_3435,
            d1.osm_id,
            d1.name,
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
        pcl.pin10,
        ARBITRARY(xy.osm_id) AS nearest_major_road_osm_id,
        ARBITRARY(xy.name) AS nearest_major_road_name,
        ARBITRARY(xy.dist_ft) AS nearest_major_road_dist_ft,
        ARBITRARY(xy.year) AS nearest_major_road_data_year,
        pcl.year
    FROM spatial.parcel AS pcl
    INNER JOIN xy_to_major_road_dist AS xy
        ON pcl.x_3435 = xy.x_3435
        AND pcl.y_3435 = xy.y_3435
        AND pcl.year = xy.pin_year
    GROUP BY pcl.pin10, pcl.year
)
