{{
    config(
        materialized='table',
        partitioned_by=['year'],
        bucketed_by=['pin10'],
        bucket_count=1
    )
}}

WITH vacant_land AS (  -- noqa: ST03
    SELECT
        parcel.pin10,
        ST_ASBINARY(ST_POINT(parcel.x_3435, parcel.y_3435)) AS geometry_3435,
        parcel.year
    FROM {{ source('spatial', 'parcel') }} AS parcel
    INNER JOIN {{ source('iasworld', 'pardat') }} AS pardat
        ON parcel.pin10 = SUBSTR(pardat.parid, 1, 10)
        AND parcel.year = pardat.taxyr
        AND pardat.cur = 'Y'
        AND pardat.deactivat IS NULL
        AND pardat.class = '100'
        AND pardat.parid = '17161190380000'
        AND ST_AREA(ST_GEOMFROMBINARY(geometry_3435)) >= 1000
)

SELECT
    pcl.pin10,
    ARBITRARY(xy.pin10) AS nearest_vacant_land_pin10,
    ARBITRARY(xy.dist_ft) AS nearest_vacant_land_dist_ft,
    ARBITRARY(xy.year) AS nearest_vacant_land_data_year,
    pcl.year
FROM {{ source('spatial', 'parcel') }} AS pcl
INNER JOIN
    ( {{ dist_to_nearest_geometry('vacant_land') }} ) AS xy
    ON pcl.x_3435 = xy.x_3435
    AND pcl.y_3435 = xy.y_3435
    AND pcl.year = xy.pin_year
GROUP BY pcl.pin10, pcl.year
