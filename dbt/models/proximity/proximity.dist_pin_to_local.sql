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
        NTILE(20) OVER () AS partition_num
    FROM spatial.traffic
    WHERE road_type = 'Local Road or Street'
),

distinct_pins AS (
    SELECT DISTINCT
        x_3435,
        y_3435,
        pin10
    FROM {{ source('spatial', 'parcel') }}
),

ranked_nearest_local AS (
    {% for partition_num in range(1, 21) %}
        SELECT
            pcl.pin10,
            xy.year,
            xy.road_name AS nearest_local_road_name,
            xy.dist_ft AS nearest_local_road_dist_ft,
            xy.year AS nearest_local_road_data_year,
            xy.daily_traffic AS nearest_local_road_daily_traffic,
            xy.speed_limit AS nearest_local_road_speed_limit,
            xy.surface_type AS nearest_local_road_surface_type,
            xy.lanes AS nearest_local_road_lanes,
            ROW_NUMBER()
                OVER (PARTITION BY pcl.pin10, xy.year ORDER BY xy.dist_ft)
                AS row_num
        FROM distinct_pins AS pcl
        INNER JOIN (
            {{ dist_to_nearest_geometry('traffic_local') }}
        ) AS xy
            ON pcl.x_3435 = xy.x_3435
            AND pcl.y_3435 = xy.y_3435
        WHERE xy.partition_num = {{ partition_num }}

        {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}
),

nearest_local AS (
    SELECT
        pin10,
        nearest_local_road_name,
        nearest_local_road_dist_ft,
        nearest_local_road_data_year,
        nearest_local_road_daily_traffic,
        nearest_local_road_speed_limit,
        nearest_local_road_surface_type,
        nearest_local_road_lanes,
        year
    FROM ranked_nearest_local
    WHERE row_num = 1
)

SELECT * FROM nearest_local
