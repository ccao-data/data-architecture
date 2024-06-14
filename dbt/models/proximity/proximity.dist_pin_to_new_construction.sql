{{
    config(
        materialized='table',
        partitioned_by=['year'],
        bucketed_by=['pin10'],
        bucket_count=1
    )
}}

WITH new_construction AS (
    SELECT
        parcel.pin10,
        ST_ASBINARY(ST_POINT(parcel.x_3435, parcel.y_3435)) AS geometry_3435,
        parcel.year,
        res_char.char_yrblt
    FROM {{ source('spatial', 'parcel') }} AS parcel
    INNER JOIN {{ source('default', 'vw_card_res_char') }} AS res_char
        ON parcel.pin10 = res_char.pin10
        AND parcel.year = res_char.year
)

SELECT
    pcl.pin10,
    ARBITRARY(xy.nearest_new_construction_pin10)
        AS nearest_new_construction_pin10,
    ARBITRARY(xy.dist_ft) AS nearest_new_construction_dist_ft,
    ARBITRARY(xy.year) AS nearest_new_construction_data_year,
    pcl.year
FROM {{ source('spatial', 'parcel') }} AS pcl
INNER JOIN
    (
        SELECT
            pcl.pin10,
            nc.pin10 AS nearest_new_construction_pin10,
            ST_DISTANCE(pcl.geometry_3435, nc.geometry_3435) AS dist_ft,
            nc.year,
            nc.char_yrblt
        FROM {{ source('spatial', 'parcel') }} AS pcl
        CROSS JOIN new_construction AS nc
        ORDER BY dist_ft
        LIMIT 1
    ) AS xy
    ON pcl.x_3435 = xy.x_3435
    AND pcl.y_3435 = xy.y_3435
    AND CAST(pcl.year AS INTEGER) >= (CAST(xy.char_yrblt AS INTEGER) + 2)
GROUP BY pcl.pin10, pcl.year
