-- CTAS to create a table of distance to the nearest CTA route for each PIN
{{
    config(
        materialized='table',
        partitioned_by=['year'],
        bucketed_by=['pin10'],
        bucket_count=1
    )
}}

WITH dist_pin_to_cta_route AS (
    WITH distinct_pins AS (
        SELECT DISTINCT
            x_3435,
            y_3435
        FROM {{ source('spatial', 'parcel') }}
    ),

    distinct_years AS (
        SELECT DISTINCT year
        FROM {{ source('spatial', 'parcel') }}
    ),

    cta_route_location AS (
        SELECT
            fill_years.pin_year,
            fill_data.*
        FROM (
            SELECT
                dy.year AS pin_year,
                MAX(df.year) AS fill_year
            FROM {{ source('spatial', 'transit_route') }} AS df
            CROSS JOIN distinct_years AS dy
            WHERE dy.year >= df.year
            GROUP BY dy.year
        ) AS fill_years
        LEFT JOIN {{ source('spatial', 'transit_route') }} AS fill_data
            ON fill_years.fill_year = fill_data.year
        WHERE fill_data.agency = 'cta'
            AND fill_data.route_type = 1
    ),

    distances AS (
        SELECT
            dp.x_3435,
            dp.y_3435,
            loc.route_id,
            loc.route_long_name,
            loc.pin_year,
            loc.year,
            ST_DISTANCE(
                ST_POINT(dp.x_3435, dp.y_3435),
                ST_GEOMFROMBINARY(loc.geometry_3435)
            ) AS distance
        FROM distinct_pins AS dp
        CROSS JOIN cta_route_location AS loc
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
        pcl.pin10,
        ARBITRARY(xy.route_id) AS nearest_cta_route_id,
        ARBITRARY(xy.route_long_name) AS nearest_cta_route_name,
        ARBITRARY(xy.dist_ft) AS nearest_cta_route_dist_ft,
        ARBITRARY(xy.year) AS nearest_cta_route_data_year,
        pcl.year
    FROM {{ source('spatial', 'parcel') }} AS pcl
    INNER JOIN xy_to_cta_route_dist AS xy
        ON pcl.x_3435 = xy.x_3435
        AND pcl.y_3435 = xy.y_3435
        AND pcl.year = xy.pin_year
    GROUP BY pcl.pin10, pcl.year
)

SELECT * FROM dist_pin_to_cta_route
