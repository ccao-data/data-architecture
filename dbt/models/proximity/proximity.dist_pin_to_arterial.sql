{{
    config(
        materialized='table',
        partitioned_by=['year'],
        bucketed_by=['pin10'],
        bucket_count=1
    )
}}

WITH arterial AS (  -- noqa: ST03
    SELECT *
    FROM {{ source('spatial', 'traffic') }}
    WHERE road_type = 'Major Arterial'
        OR road_type = 'Minor Arterial'
),

SELECT
    pcl.pin10,
    xy.year,
    ARBITRARY(xy.road_name) AS nearest_arterial_road_name,
    ARBITRARY(xy.dist_ft) AS nearest_arterial_road_dist_ft,
    ARBITRARY(xy.year) AS nearest_arterial_road_data_year,
    ARBITRARY(xy.daily_traffic) AS nearest_arterial_road_daily_traffic,
    ARBITRARY(xy.speed_limit) AS nearest_arterial_road_speed_limit,
    ARBITRARY(xy.surface_type) AS nearest_arterial_road_surface_type,
    ARBITRARY(xy.lanes) AS nearest_arterial_road_lanes
FROM {{ source('spatial', 'parcel') }} AS pcl
INNER JOIN ( {{ dist_to_nearest_geometry('arterial') }} ) AS xy
    ON pcl.x_3435 = xy.x_3435
    AND pcl.y_3435 = xy.y_3435
    AND pcl.year = xy.pin_year
GROUP BY pcl.pin10, xy.year
