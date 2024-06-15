-- CTAS to create a table of distance to the nearest Metra stop for each PIN
{{
    config(
        materialized='table',
        partitioned_by=['year'],
        bucketed_by=['pin10'],
        bucket_count=1
    )
}}

WITH stadium AS (  -- noqa: ST03
    SELECT
        ST_ASBINARY(ST_POINT(stadium.x_3435, stadium.y_3435)) AS geometry_3435,
        stadium.name,  -- noqa: RF03
        stadium.date_opened,  -- noqa: RF03
        stadium.year  -- noqa: RF03
    FROM {{ ref('spatial.stadium') }}
)

SELECT
    pcl.pin10,
    ARBITRARY(xy.name) AS nearest_stadium_name,
    ARBITRARY(xy.dist_ft) AS nearest_stadium_dist_ft,
    ARBITRARY(xy.year) AS nearest_stadium_data_year,
    ARBITRARY(xy.date_opened) AS nearest_stadium_date_opened,
    pcl.year
FROM {{ source('spatial', 'parcel') }} AS pcl
INNER JOIN ( {{ dist_to_nearest_geometry('stadium') }} ) AS xy
    ON pcl.x_3435 = xy.x_3435
    AND pcl.y_3435 = xy.y_3435
GROUP BY pcl.pin10, pcl.year;
