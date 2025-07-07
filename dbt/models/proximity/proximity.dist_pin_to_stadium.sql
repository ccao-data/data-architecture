-- CTAS to create a table of distance to the nearest stadium for each PIN
{{
    config(
        materialized='table',
        partitioned_by=['year'],
        bucketed_by=['pin10'],
        bucket_count=1
    )
}}

WITH xy AS (
    {{ dist_to_nearest_geometry(ref('spatial.stadium'), geometry_type = "point") }}
)

SELECT
    pcl.pin10,
    ARBITRARY(xy.name) AS nearest_stadium_name,
    ARBITRARY(xy.dist_ft) AS nearest_stadium_dist_ft,
    ARBITRARY(xy.year) AS nearest_stadium_data_year,
    pcl.year
FROM {{ source('spatial', 'parcel') }} AS pcl
INNER JOIN xy
    ON pcl.x_3435 = xy.x_3435
    AND pcl.y_3435 = xy.y_3435
    AND pcl.year = xy.pin_year
GROUP BY pcl.pin10, pcl.year
