{{ config(
    materialized='table',
    partitioned_by=['year'],
    bucketed_by=['pin10'],
    bucket_count=1
) }}

WITH stadiums AS (
    SELECT
        CASE
            WHEN pin10 = '1420227002' THEN 'Wrigley'
            WHEN pin10 = '1718201035' THEN 'United Center'
            WHEN pin10 = '1722110002' THEN 'Soldier Field'
            WHEN pin10 = '1733400049' THEN 'Guaranteed Rate Field'
            WHEN pin10 = '1722320018' THEN 'Wintrust Arena'
            WHEN pin10 = '1717239022' THEN 'UIC Pavilion'
        END AS stadium_name,
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
        AND year >= '2017'
),

distances AS (
    SELECT
        pcl.pin10,
        st.stadium_name,
        st.pin10_stadium,
        SQRT(
            POW(st.x_3435_stadium - pcl.x_3435, 2)
            + POW(st.y_3435_stadium - pcl.y_3435, 2)
        ) AS dist_ft,
        pcl.year
    FROM
        stadiums AS st
    CROSS JOIN {{ source('spatial', 'parcel') }} AS pcl
    WHERE
        pcl.year = st.year
),

nearest_stadiums AS (
    SELECT
        pin10,
        stadium_name,
        dist_ft,
        year,
        ROW_NUMBER() OVER (PARTITION BY pin10, year ORDER BY dist_ft) AS rn
    FROM
        distances
),

deduplicated AS (
    SELECT
        pin10,
        nearest_stadium_dist_ft,
        nearest_stadium_name,
        year,
        ROW_NUMBER()
            OVER (PARTITION BY year ORDER BY nearest_stadium_dist_ft)
            AS year_rn
    FROM (
        SELECT
            ns.pin10,
            ns.dist_ft AS nearest_stadium_dist_ft,
            ns.stadium_name AS nearest_stadium_name,
            ns.year
        FROM
            nearest_stadiums AS ns
        WHERE
            ns.rn = 1
    ) AS base_query
)

SELECT
    pin10,
    nearest_stadium_dist_ft,
    nearest_stadium_name,
    year
FROM
    deduplicated
WHERE
    year_rn = 1
