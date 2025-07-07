-- CTAS to create a table of distance to the nearest Metra stop for each PIN
{{
    config(
        materialized='table',
        partitioned_by=['year'],
        bucketed_by=['pin10'],
        bucket_count=1
    )
}}

WITH metra_stop AS (  -- noqa: ST03
    SELECT *
    FROM {{ source('spatial', 'transit_stop') }}
    WHERE agency = 'metra'
        AND route_type = 2
)

SELECT
    pcl.pin10,
    ARBITRARY(xy.stop_id) AS nearest_metra_stop_id,
    ARBITRARY(xy.stop_name) AS nearest_metra_stop_name,
    ARBITRARY(xy.dist_ft) AS nearest_metra_stop_dist_ft,
    ARBITRARY(xy.year) AS nearest_metra_stop_data_year,
    pcl.year
FROM {{ source('spatial', 'parcel') }} AS pcl
INNER JOIN
    ( {{ dist_to_nearest_geometry('metra_stop', geometry_type = "point") }} )
        AS xy
    ON pcl.x_3435 = xy.x_3435
    AND pcl.y_3435 = xy.y_3435
    AND pcl.year = xy.pin_year
GROUP BY pcl.pin10, pcl.year
