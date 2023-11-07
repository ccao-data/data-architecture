-- CTAS to create a table of distance to the nearest park (2+ acre) for each PIN
{{
    config(
        materialized='table',
        partitioned_by=['year'],
        bucketed_by=['pin10'],
        bucket_count=1
    )
}}

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

park_location AS (
    SELECT
        fill_years.pin_year,
        fill_data.*
    FROM (
        SELECT
            dy.year AS pin_year,
            MAX(df.year) AS fill_year
        FROM {{ source('spatial', 'park') }} AS df
        CROSS JOIN distinct_years AS dy
        WHERE dy.year >= df.year
        GROUP BY dy.year
    ) AS fill_years
    LEFT JOIN {{ source('spatial', 'park') }} AS fill_data
        ON fill_years.fill_year = fill_data.year
    WHERE ST_AREA(ST_GEOMFROMBINARY(fill_data.geometry_3435)) > 87120
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
    CROSS JOIN park_location AS loc
),

xy_to_park_dist AS (
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
    ARBITRARY(xy.osm_id) AS nearest_park_osm_id,
    ARBITRARY(xy.name) AS nearest_park_name,
    ARBITRARY(xy.dist_ft) AS nearest_park_dist_ft,
    ARBITRARY(xy.year) AS nearest_park_data_year,
    pcl.year
FROM {{ source('spatial', 'parcel') }} AS pcl
INNER JOIN xy_to_park_dist AS xy
    ON pcl.x_3435 = xy.x_3435
    AND pcl.y_3435 = xy.y_3435
    AND pcl.year = xy.pin_year
GROUP BY pcl.pin10, pcl.year
