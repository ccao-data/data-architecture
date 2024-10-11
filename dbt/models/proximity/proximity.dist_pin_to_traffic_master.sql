{{
    config(
        materialized='table',
        partitioned_by=['year'],
        bucketed_by=['pin10'],
        bucket_count=1
    )
}}

WITH traffic_highway AS (
    -- Local Road or Street traffic data
    SELECT *
    FROM {{ source('spatial', 'traffic') }}
    WHERE daily_traffic > 0
        AND daily_traffic IS NOT NULL
        AND road_type = 'Freeway And Expressway'
),

traffic_minor_arterial AS (
    -- Minor Arterial traffic data
    SELECT *
    FROM {{ source('spatial', 'traffic') }}
    WHERE daily_traffic > 0
        AND daily_traffic IS NOT NULL
        AND road_type = 'Minor Arterial'
),

distinct_pins AS (
    -- Select distinct pins from the parcel dataset
    SELECT DISTINCT
        x_3435,
        y_3435,
        pin10,
        year
    FROM {{ source('spatial', 'parcel') }}
)

SELECT
    pcl.pin10,
    -- Nearest local road values
    ARBITRARY(local_xy.road_name) AS nearest_local_road_name,
    ARBITRARY(local_xy.dist_ft) AS nearest_local_road_dist_ft,
    ARBITRARY(local_xy.year) AS nearest_local_road_data_year,
    ARBITRARY(local_xy.daily_traffic) AS nearest_local_road_daily_traffic,
    -- Nearest minor arterial values
    ARBITRARY(arterial_xy.road_name) AS nearest_arterial_road_name,
    ARBITRARY(arterial_xy.dist_ft) AS nearest_arterial_road_dist_ft,
    ARBITRARY(arterial_xy.year) AS nearest_arterial_road_data_year,
    ARBITRARY(arterial_xy.daily_traffic) AS nearest_arterial_road_daily_traffic,
    pcl.year
FROM distinct_pins AS pcl
-- Join for nearest local road
LEFT JOIN traffic_highway AS local_xy
    ON pcl.x_3435 = local_xy.x_3435
    AND pcl.y_3435 = local_xy.y_3435
    AND pcl.year = local_xy.pin_year
-- Join for nearest minor arterial
LEFT JOIN
    traffic_minor_arterial AS arterial_xy
    ON pcl.x_3435 = arterial_xy.x_3435
    AND pcl.y_3435 = arterial_xy.y_3435
    AND pcl.year = arterial_xy.pin_year
GROUP BY pcl.pin10, pcl.year
