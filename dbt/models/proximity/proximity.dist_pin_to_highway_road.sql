{{
    config(
        materialized='table',
        partitioned_by=['year'],
        bucketed_by=['pin10'],
        bucket_count=1
    )
}}

WITH highway AS (  -- noqa: ST03
    SELECT *
    FROM {{ source('spatial', 'traffic') }}
    WHERE road_type = 'Interstate'
        OR road_type = 'Freeway and Expressway'
)

SELECT
    pcl.pin10,
    ARBITRARY(xy.road_name) AS nearest_highway_road_name,
    ARBITRARY(xy.dist_ft) AS nearest_highway_road_dist_ft,
    ARBITRARY(xy.daily_traffic) AS nearest_highway_road_daily_traffic,
    ARBITRARY(xy.speed_limit) AS nearest_highway_road_speed_limit,
    ARBITRARY(xy.surface_type) AS nearest_highway_road_surface_type,
    ARBITRARY(xy.lanes) AS nearest_highway_road_lanes,
    ARBITRARY(xy.year) AS nearest_highway_road_data_year,
    pcl.year
FROM {{ source('spatial', 'parcel') }} AS pcl
INNER JOIN ( {{ dist_to_nearest_geometry('highway') }} ) AS xy
    ON pcl.x_3435 = xy.x_3435
    AND pcl.y_3435 = xy.y_3435
    AND pcl.year = xy.pin_year
GROUP BY pcl.pin10, pcl.year
