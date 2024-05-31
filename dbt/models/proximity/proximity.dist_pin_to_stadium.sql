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
            '1420227002',  -- Wrigley
            '1718201035',  -- United Center
            '1722110002',  -- Soldier Field
            '1733400049',  -- Guaranteed Rate Field
            '1722320018',  -- Wintrust Arena
            '1717239022'   -- UIC Pavilion
        )
        AND year = '2023'
)

SELECT
    pcl.pin10,
    MIN(output.dist_ft) AS nearest_stadium_dist_ft,
    CASE
        WHEN ANY_VALUE(output.pin10_stadium) = '1420227002' THEN 'Wrigley'
        WHEN ANY_VALUE(output.pin10_stadium) = '1718201035' THEN 'United Center'
        WHEN ANY_VALUE(output.pin10_stadium) = '1722110002' THEN 'Soldier Field'
        WHEN
            ANY_VALUE(output.pin10_stadium) = '1733400049'
            THEN 'Guaranteed Rate Field'
        WHEN
            ANY_VALUE(output.pin10_stadium) = '1722320018'
            THEN 'Wintrust Arena'
        WHEN ANY_VALUE(output.pin10_stadium) = '1717239022' THEN 'UIC Pavilion'
        ELSE 'Unknown'
    END AS nearest_stadium_name,
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
        pcl.year >= st.year
) AS output
    ON pcl.pin10 = output.parcel_pin10
    AND pcl.year >= output.year
GROUP BY pcl.pin10, pcl.year;
