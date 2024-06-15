{{
    config(
        materialized='table',
        partitioned_by=['year'],
        bucketed_by=['pin10'],
        bucket_count=1
    )
}}

WITH new_permit AS (
    SELECT
        parcel.pin10,
        ST_ASBINARY(ST_POINT(parcel.x_3435, parcel.y_3435)) AS geometry_3435,
        parcel.year,
        iasworld.permdt AS permit_date,
        iasworld.amount
    FROM spatial.parcel AS parcel
    INNER JOIN iasworld.permit AS iasworld
        ON parcel.pin10 = iasworld.parid
        AND CAST(parcel.year AS INT)
        >= CAST(SUBSTRING(iasworld.permdt, 1, 4) AS INT)
),

min_year_per_pin AS (
    SELECT
        pin10,
        MIN(year) AS min_year
    FROM new_permit
    GROUP BY pin10
)

SELECT
    pcl.pin10,
    ARBITRARY(xy.pin10) AS nearest_permit_pin10,
    ARBITRARY(xy.dist_ft) AS nearest_permit_dist_ft,
    ARBITRARY(xy.year) AS nearest_permit_data_year,
    ARBITRARY(xy.permit_date) AS nearest_permit_year,
    pcl.year
FROM {{ source('spatial', 'parcel') }} AS pcl
INNER JOIN (
    SELECT
        np.pin10,
        np.geometry_3435,
        np.year,
        np.permit_date,
        np.amount
    FROM new_permit AS np
    INNER JOIN min_year_per_pin AS myp
        ON np.pin10 = myp.pin10
        AND np.year = myp.min_year
) AS xy
    ON pcl.x_3435 = xy.x_3435
    AND pcl.y_3435 = xy.y_3435
    AND pcl.year = xy.year
GROUP BY pcl.pin10, pcl.year
