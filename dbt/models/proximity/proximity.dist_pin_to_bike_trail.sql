-- CTAS to create a table of distance to the nearest bike trail for each PIN
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
    ARBITRARY(xy.street) AS nearest_bike_trail_id,
    ARBITRARY(xy.name) AS nearest_bike_trail_name,
    ARBITRARY(xy.dist_ft) AS nearest_bike_trail_dist_ft,
    ARBITRARY(xy.year) AS nearest_bike_trail_data_year,
    pcl.year
FROM {{ source('spatial', 'parcel') }} AS pcl
INNER JOIN
    ( {{ dist_to_nearest_geometry(source('spatial', 'bike_trail')) }} ) AS xy
    ON pcl.x_3435 = xy.x_3435
    AND pcl.y_3435 = xy.y_3435
    AND pcl.year = xy.pin_year
GROUP BY pcl.pin10, pcl.year
