-- CTAS to create a table of distance to the nearest CTA route for each PIN
{{
    config(
        materialized='table',
        partitioned_by=['year'],
        bucketed_by=['pin10'],
        bucket_count=1
    )
}}

WITH cta_route AS (
    SELECT *
    FROM {{ source('spatial', 'transit_route') }}
    WHERE agency = 'cta'
        AND route_type = 1
)

SELECT
    pcl.pin10,
    ARBITRARY(xy.route_id) AS nearest_cta_route_id,
    ARBITRARY(xy.route_long_name) AS nearest_cta_route_name,
    ARBITRARY(xy.dist_ft) AS nearest_cta_route_dist_ft,
    ARBITRARY(xy.year) AS nearest_cta_route_data_year,
    pcl.year
FROM {{ source('spatial', 'parcel') }} AS pcl
INNER JOIN ( {{ dist_to_nearest_geometry('cta_route') }} ) AS xy
    ON pcl.x_3435 = xy.x_3435
    AND pcl.y_3435 = xy.y_3435
    AND pcl.year = xy.pin_year
GROUP BY pcl.pin10, pcl.year
