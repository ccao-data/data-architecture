-- CTAS to create a table of distance to the stadiums for each PIN
{{
    config(
        materialized='table',
        partitioned_by=['year'],
        bucketed_by=['pin10'],
        bucket_count=1
    )
}}

WITH distinct_pins AS (
    SELECT DISTINCT
        pin10,
        x_3435,
        y_3435,
        year
    FROM {{ source('spatial', 'parcel') }}
),

xy_to_stadium_dist AS (
    SELECT
        dp.pin10,
        dp.x_3435,
        dp.y_3435,
        dp.year,
        'Soldier Field' AS nearest_stadium_name,
        ST_DISTANCE(
            ST_POINT(dp.x_3435, dp.y_3435), ST_POINT(1179603, 1893122)
        ) AS nearest_stadium_dist_ft
    FROM distinct_pins AS dp
    UNION ALL
    SELECT
        dp.pin10,
        dp.x_3435,
        dp.y_3435,
        dp.year,
        'Wintrust Arena' AS nearest_stadium_name,
        ST_DISTANCE(
            ST_POINT(dp.x_3435, dp.y_3435), ST_POINT(1178259, 1889955)
        ) AS nearest_stadium_dist_ft
    FROM distinct_pins AS dp
    UNION ALL
    SELECT
        dp.pin10,
        dp.x_3435,
        dp.y_3435,
        dp.year,
        'Guaranteed Rate Field' AS nearest_stadium_name,
        ST_DISTANCE(
            ST_POINT(dp.x_3435, dp.y_3435), ST_POINT(1174703, 1881406)
        ) AS nearest_stadium_dist_ft
    FROM distinct_pins AS dp
    UNION ALL
    SELECT
        dp.pin10,
        dp.x_3435,
        dp.y_3435,
        dp.year,
        'Wrigley Field' AS nearest_stadium_name,
        ST_DISTANCE(
            ST_POINT(dp.x_3435, dp.y_3435), ST_POINT(1168634, 1924426)
        ) AS nearest_stadium_dist_ft
    FROM distinct_pins AS dp
    UNION ALL
    SELECT
        dp.pin10,
        dp.x_3435,
        dp.y_3435,
        dp.year,
        'United Center' AS nearest_stadium_name,
        ST_DISTANCE(
            ST_POINT(dp.x_3435, dp.y_3435), ST_POINT(1163584, 1899780)
        ) AS nearest_stadium_dist_ft
    FROM distinct_pins AS dp
    UNION ALL
    SELECT
        dp.pin10,
        dp.x_3435,
        dp.y_3435,
        dp.year,
        'UIC Pavilion' AS nearest_stadium_name,
        ST_DISTANCE(
            ST_POINT(dp.x_3435, dp.y_3435), ST_POINT(1168816, 1897726)
        ) AS nearest_stadium_dist_ft
    FROM distinct_pins AS dp
),

nearest_min_stadium_dist AS (
    SELECT
        pin10,
        x_3435,
        y_3435,
        year,
        nearest_stadium_name,
        nearest_stadium_dist_ft,
        ROW_NUMBER()
            OVER (PARTITION BY pin10, year ORDER BY nearest_stadium_dist_ft)
            AS rnk
    FROM xy_to_stadium_dist
)

SELECT
    pin10,
    nearest_stadium_name,
    nearest_stadium_dist_ft AS min_stadium_dist_ft,
    year AS nearest_stadium_data_year
FROM nearest_min_stadium_dist
WHERE rnk = 1;
