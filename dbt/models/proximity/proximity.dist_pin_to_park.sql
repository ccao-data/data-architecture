-- CTAS to create a table of distance to the nearest park (2+ acre) for each PIN
{{
    config(
        materialized='table',
        partitioned_by=['year'],
        bucketed_by=['pin10'],
        bucket_count=1
    )
}}

WITH parks AS (  -- noqa: ST03
    SELECT *
    FROM {{ source('spatial', 'park') }}
    WHERE ST_AREA(ST_GEOMFROMBINARY(geometry_3435)) > 87120
)

SELECT
    pcl.pin10,
    ARBITRARY(xy.osm_id) AS nearest_park_osm_id,
    ARBITRARY(xy.name) AS nearest_park_name,
    ARBITRARY(xy.dist_ft) AS nearest_park_dist_ft,
    ARBITRARY(xy.year) AS nearest_park_data_year,
    pcl.year
FROM {{ source('spatial', 'parcel') }} AS pcl
INNER JOIN ( {{ dist_to_nearest_geometry('parks') }} ) AS xy
    ON pcl.x_3435 = xy.x_3435
    AND pcl.y_3435 = xy.y_3435
    AND pcl.year = xy.pin_year
GROUP BY pcl.pin10, pcl.year
