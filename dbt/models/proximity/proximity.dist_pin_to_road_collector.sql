{{
    config(
        materialized='table',
        partitioned_by=['year'],
        bucketed_by=['pin10'],
        bucket_count=1
    )
}}

WITH collector AS (  -- noqa: ST03
    SELECT *
    FROM {{ source('spatial', 'road') }}
    WHERE road_type IN ('Major Collector', 'Minor Collector')
)

SELECT
    pcl.pin10,
    ARBITRARY(xy.road_name) AS nearest_road_collector_name,
    ARBITRARY(xy.dist_ft) AS nearest_road_collector_dist_ft,
    ARBITRARY(xy.daily_traffic) AS nearest_road_collector_daily_traffic,
    ARBITRARY(xy.speed_limit) AS nearest_road_collector_speed_limit,
    ARBITRARY(xy.surface_type) AS nearest_road_collector_surface_type,
    ARBITRARY(xy.lanes) AS nearest_road_collector_lanes,
    ARBITRARY(xy.year) AS nearest_road_collector_data_year,
    pcl.year
FROM {{ source('spatial', 'parcel') }} AS pcl
INNER JOIN ( {{ dist_to_nearest_geometry('collector') }} ) AS xy
    ON pcl.x_3435 = xy.x_3435
    AND pcl.y_3435 = xy.y_3435
    AND pcl.year = xy.pin_year
GROUP BY pcl.pin10, pcl.year
