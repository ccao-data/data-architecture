-- CTAS to create a table of distance to the nearest road type 
-- (minor arterial and highway) for each PIN
{{
    config(
        materialized='table',
        partitioned_by=['year'],
        bucketed_by=['pin10'],
        bucket_count=1
    )
}}

WITH traffic_minor AS (  -- noqa: ST03
    SELECT *
    FROM {{ source('spatial', 'traffic') }}
    WHERE daily_traffic > 0
        AND daily_traffic IS NOT NULL
        AND road_type = 'Minor Arterial'
),

traffic_highway AS (  -- noqa: ST03
    SELECT *
    FROM {{ source('spatial', 'traffic') }}
    WHERE daily_traffic > 0
        AND daily_traffic IS NOT NULL
        AND (
            road_type = 'Interstate'
            OR road_type = 'Freeway And Expressway'
        )
),

distinct_pins AS (
    SELECT DISTINCT
        x_3435,
        y_3435,
        pin10,
        year
    FROM {{ source('spatial', 'parcel') }}
),

-- Select nearest road from Minor Arterial
nearest_minor AS (
    SELECT
        pcl.pin10,
        ARBITRARY(xy.road_name) AS nearest_road_name,
        ARBITRARY(xy.dist_ft) AS nearest_road_dist_ft,
        ARBITRARY(xy.year) AS nearest_road_data_year,
        ARBITRARY(xy.daily_traffic) AS nearest_road_daily_traffic,
        'Minor Arterial' AS road_type,
        pcl.year
    FROM distinct_pins AS pcl
    INNER JOIN ( {{ dist_to_nearest_geometry('traffic_minor') }} ) AS xy
        ON pcl.x_3435 = xy.x_3435
        AND pcl.y_3435 = xy.y_3435
        AND pcl.year = xy.pin_year
    GROUP BY pcl.pin10, pcl.year
),

-- Select nearest road from Highway (Interstate, Freeway, Expressway)
nearest_highway AS (
    SELECT
        pcl.pin10,
        ARBITRARY(xy.road_name) AS nearest_road_name,
        ARBITRARY(xy.dist_ft) AS nearest_road_dist_ft,
        ARBITRARY(xy.year) AS nearest_road_data_year,
        ARBITRARY(xy.daily_traffic) AS nearest_road_daily_traffic,
        'Highway' AS road_type,
        pcl.year
    FROM distinct_pins AS pcl
    INNER JOIN ( {{ dist_to_nearest_geometry('traffic_highway') }} ) AS xy
        ON pcl.x_3435 = xy.x_3435
        AND pcl.y_3435 = xy.y_3435
        AND pcl.year = xy.pin_year
    GROUP BY pcl.pin10, pcl.year
)

-- Combine the two results with UNION ALL
SELECT *
FROM nearest_minor
UNION ALL
SELECT *
FROM nearest_highway
