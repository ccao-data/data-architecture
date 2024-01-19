-- CTAS that finds the 3 nearest neighbor PINs for every PIN for every year
-- within a 1km radius

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
    ARBITRARY(xy.name) AS nearest_hospital_name,
    ARBITRARY(xy.dist_ft) AS nearest_vacant_dist_ft,
    ARBITRARY(xy.year) AS nearest_vacant_data_year,
    pcl.year
FROM {{ source('spatial', 'parcel') }} AS pcl
INNER JOIN
    (
        SELECT * 
        FROM {{ dist_to_nearest_geometry(source('spatial', 'parcel')) }} AS parcel
            WHERE EXISTS (
                SELECT 1
                FROM default.vw_pin_universe AS universe
                WHERE universe.pin10 = parcel.pin10
                AND universe.class = '100' 
)
    ) AS xy
    ON pcl.x_3435 = xy.x_3435
    AND pcl.y_3435 = xy.y_3435
    AND pcl.year = xy.pin_year
GROUP BY pcl.pin10, pcl.year
