{{
    config(
        materialized='table',
        partitioned_by=['year'],
        bucketed_by=['pin10'],
        bucket_count=1
    )
}}

WITH new_permit AS (  -- noqa: ST03
    SELECT
        parcel.pin10,
        ST_ASBINARY(ST_POINT(parcel.x_3435, parcel.y_3435)) AS geometry_3435,
        parcel.year,
        iasworld.permdt AS permit_date,
        iasworld.amount,
        iasworld.parid
    FROM spatial.parcel AS parcel
    INNER JOIN iasworld.permit AS iasworld
        ON parcel.pin10 = (SUBSTRING(iasworld.parid, 1, 10))
        AND (
            CAST(parcel.year AS INT)
            - CAST(SUBSTRING(iasworld.permdt, 1, 4) AS INT)
        ) BETWEEN 0 AND 2
        AND iasworld.amount >= 50000
)

SELECT
    pcl.pin10,
    ARBITRARY(xy.pin10) AS nearest_permit_pin10,
    ARBITRARY(xy.dist_ft) AS nearest_permit_dist_ft,
    ARBITRARY(xy.year) AS nearest_permit_data_year,
    ARBITRARY(xy.permit_date) AS nearest_permit_date,
    pcl.year
FROM {{ source('spatial', 'parcel') }} AS pcl
INNER JOIN
    ( {{ dist_to_nearest_geometry('new_permit') }} ) AS xy
    ON pcl.x_3435 = xy.x_3435
    AND pcl.y_3435 = xy.y_3435
    AND pcl.year = xy.pin_year
GROUP BY pcl.pin10, pcl.year
