-- CTAS to create a table of distance to the nearest hospital for each PIN
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
    CAST(CAST(ARBITRARY(xy.gniscode) AS BIGINT) AS VARCHAR)
        AS nearest_hospital_gnis_code,
    ARBITRARY(xy.name) AS nearest_hospital_name,
    ARBITRARY(xy.dist_ft) AS nearest_hospital_dist_ft,
    ARBITRARY(xy.year) AS nearest_hospital_data_year,
    pcl.year
FROM {{ source('spatial', 'parcel') }} AS pcl
INNER JOIN ( {{ dist_to_nearest_geometry(source('spatial', 'hospital')) }} ) AS xy
    ON pcl.x_3435 = xy.x_3435
    AND pcl.y_3435 = xy.y_3435
    AND pcl.year = xy.pin_year
GROUP BY pcl.pin10, pcl.year
