-- Create a table of distance to the nearest stadium for each PIN

{{ config(
    materialized='table',
    partitioned_by=['year'],
    bucketed_by=['pin10'],
    bucket_count=1
) }}

WITH stadiums AS (
    SELECT
        pin10 AS pin10_stadium,
        x_3435 AS x_3435_stadium,
        y_3435 AS y_3435_stadium,
        year
    FROM
        {{ source('spatial', 'parcel') }}
    WHERE
        pin10 IN (
            '1420227002',
            '1718201035',
            '1722110002',
            '1733400049',
            '1722320018',
            '1717239022'
        )
        AND year = (
            SELECT MAX(year)
            FROM {{ source('spatial', 'parcel') }}
        )
)

SELECT
    pcl.pin10,
    MIN(output.dist_ft) AS nearest_stadium_dist_ft,
    pcl.year
FROM {{ source('spatial', 'parcel') }} AS pcl
INNER JOIN (
    SELECT
        st.pin10_stadium,
        pcl.pin10 AS parcel_pin10,
        st.year,
        SQRT(
            POW(st.x_3435_stadium - pcl.x_3435, 2)
            + POW(st.y_3435_stadium - pcl.y_3435, 2)
        ) AS dist_ft
    FROM
        stadiums AS st, {{ source('spatial', 'parcel') }} AS pcl
    WHERE
        pcl.year = st.year
) AS output
    ON pcl.pin10 = output.parcel_pin10
    AND pcl.year = output.year
GROUP BY pcl.pin10, pcl.year;
