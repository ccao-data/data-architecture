-- CTAS to create a table of distance to the nearest rail tracks for each PIN
{{
    config(
        materialized='table',
        partitioned_by=['year'],
        bucketed_by=['pin10'],
        bucket_count=1
    )
}}

SELECT
    pcl.pin10,
    ARBITRARY(xy.name_id) AS nearest_road_name,
    ARBITRARY(xy.dist_ft) AS nearest_speed_limit_dist_ft,
    ARBITRARY(xy.year) AS nearest_speed_limit_data_year,
    pcl.year
FROM {{ source('spatial', 'parcel') }} AS pcl
INNER JOIN
    ( {{ dist_to_nearest_geometry(source('spatial', 'traffic')) }} ) AS xy
    ON pcl.x_3435 = xy.x_3435
    AND pcl.y_3435 = xy.y_3435
    AND pcl.year = xy.pin_year
    AND xy.sp_lim > 0
GROUP BY pcl.pin10, pcl.year
