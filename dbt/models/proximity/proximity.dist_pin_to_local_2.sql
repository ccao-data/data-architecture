{{
    config(
        materialized='table',
        partitioned_by=['year'],
        bucketed_by=['pin10'],
        bucket_count=1
    )
}}

WITH traffic_local AS ( -- noqa: ST03
    SELECT
        *,
        -- Cast traffic geometry to the correct type
        ST_GEOMFROMBINARY(geometry_3435) AS geometry_cast
    FROM {{ source('spatial', 'traffic') }}
    WHERE road_type = 'Local Road or Street'
),

township_geometry AS ( -- noqa: ST03
    SELECT
        *,
        -- Cast township geometry to the correct type
        ST_GEOMFROMBINARY(geometry_3435) AS geometry_cast
    FROM {{ source('spatial', 'township') }}
),

joined_traffic_township AS ( -- noqa: ST03
    SELECT
        tl.*,
        town.township_code,
        ST_DISTANCE(tl.geometry_cast, town.geometry_cast)
            AS distance_to_township
    FROM traffic_local AS tl
    INNER JOIN township_geometry AS town
        ON ST_INTERSECTS(tl.geometry_cast, town.geometry_cast)
),

distinct_pins AS ( -- noqa: ST03
    SELECT DISTINCT
        x_3435,
        y_3435,
        town_code,
        pin10
    FROM {{ source('spatial', 'parcel') }}
)

SELECT
    pcl.pin10,
    xy.road_name AS nearest_local_road_name,
    xy.dist_ft AS nearest_local_road_dist_ft,
    xy.year AS nearest_local_road_data_year,
    xy.daily_traffic AS nearest_local_road_daily_traffic,
    xy.speed_limit AS nearest_local_road_speed_limit,
    xy.surface_type AS nearest_local_road_surface_type,
    xy.lanes AS nearest_local_road_lanes,
    xy.year
FROM distinct_pins AS pcl
INNER JOIN (
    {{ dist_to_nearest_geometry('joined_traffic_township') }}
) AS xy
    ON pcl.x_3435 = xy.x_3435
    AND pcl.y_3435 = xy.y_3435
    AND pcl.town_code = xy.township_code
