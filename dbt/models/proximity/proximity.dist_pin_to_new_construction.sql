{{
    config(
        materialized='table',
        partitioned_by=['year'],
        bucketed_by=['pin10'],
        bucket_count=1
    )
}}

WITH new_construction AS (  -- noqa: ST03
    SELECT
        parcel.pin10,
        ST_ASBINARY(ST_POINT(parcel.x_3435, parcel.y_3435)) AS geometry_3435,
        parcel.year,
        vw_card_res_char.char_yrblt
    FROM spatial.parcel AS parcel
    INNER JOIN {{ ref('default.vw_card_res_char') }} AS vw_card_res_char
        ON parcel.pin10 = vw_card_res_char.pin10
        AND parcel.year = vw_card_res_char.year
        AND CAST(parcel.year AS INT)
        <= CAST(vw_card_res_char.char_yrblt AS INT) + 3
)
SELECT
    pcl.pin10,
    ARBITRARY(xy.pin10) AS nearest_new_construction_pin10,
    ARBITRARY(xy.dist_ft) AS nearest_new_construction_dist_ft,
    ARBITRARY(xy.year) AS nearest_new_construction_data_year,
    ARBITRARY(xy.char_yrblt) AS nearest_new_construction_char_yrblt,
    pcl.year
FROM {{ source('spatial', 'parcel') }} AS pcl
INNER JOIN
    ( {{ dist_to_nearest_geometry('new_construction') }} ) AS xy
    ON pcl.x_3435 = xy.x_3435
    AND pcl.y_3435 = xy.y_3435
    AND pcl.year = xy.pin_year
GROUP BY pcl.pin10, pcl.year
