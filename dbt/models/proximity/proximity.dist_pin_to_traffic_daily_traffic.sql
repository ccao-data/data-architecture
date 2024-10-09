-- CTAS to create a table of distance to the nearest Metra route for each PIN
{{
    config(
        materialized='table',
        partitioned_by=['year'],
        bucketed_by=['pin10'],
        bucket_count=1
    )
}}

WITH traffic AS (  -- noqa: ST03
    SELECT *
    FROM {{ source('spatial', 'traffic') }}
    WHERE annual_traffic > 0
        AND annual_traffic IS NOT NULL
)

SELECT
    pcl.pin10,
    ARBITRARY(xy.road_name) AS nearest_road_name,
    ARBITRARY(xy.dist_ft) AS nearest_road_dist_ft,
    ARBITRARY(xy.year) AS nearest_road_data_year,
    ARBITRARY(xy.annual_traffic) AS nearest_road_annual_traffic,
    pcl.year
FROM {{ source('spatial', 'parcel') }} AS pcl
INNER JOIN ( {{ dist_to_nearest_geometry('traffic') }} ) AS xy
    ON pcl.x_3435 = xy.x_3435
    AND pcl.y_3435 = xy.y_3435
    AND pcl.year = xy.pin_year
GROUP BY pcl.pin10, pcl.year
