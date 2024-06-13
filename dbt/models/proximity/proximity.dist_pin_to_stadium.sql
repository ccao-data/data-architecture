-- CTAS to create a table of distance to the stadiums for each PIN
{{
    config(
        materialized='table',
        partitioned_by=['year'],
        bucketed_by=['pin10'],
        bucket_count=1
    )
}}

-- Define stadium names and locations
WITH stadiums AS (
    SELECT
        'Soldier Field' AS stadium_name,
        ST_POINT(1179603, 1893122) AS stadium_xy
    UNION ALL
    SELECT
        'Wintrust Arena' AS stadium_name,
        ST_POINT(1178259, 1889955) AS stadium_xy
    UNION ALL
    SELECT
        'Guaranteed Rate Field' AS stadium_name,
        ST_POINT(1174703, 1881406) AS stadium_xy
    UNION ALL
    SELECT
        'Wrigley Field' AS stadium_name,
        ST_POINT(1168634, 1924426) AS stadium_xy
    UNION ALL
    SELECT
        'United Center' AS stadium_name,
        ST_POINT(1163584, 1899780) AS stadium_xy
    UNION ALL
    SELECT
        'UIC Pavilion' AS stadium_name,
        ST_POINT(1168816, 1897726) AS stadium_xy
),

-- Calculate distance between every distinct parcel and each stadium
xy_to_stadium_dist AS (
    SELECT DISTINCT
        parcel.pin10,
        parcel.x_3435,
        parcel.y_3435,
        parcel.year,
        stadiums.stadium_name,
        ST_DISTANCE(
            ST_POINT(parcel.x_3435, parcel.y_3435), stadiums.stadium_xy
        ) AS stadium_dist_ft
    FROM spatial.parcel
    CROSS JOIN stadiums
),

-- Rank distance to stadium within parcel
min_stadium_dist AS (
    SELECT
        pin10,
        x_3435,
        y_3435,
        year,
        stadium_name,
        stadium_dist_ft,
        ROW_NUMBER()
            OVER (PARTITION BY pin10, year ORDER BY stadium_dist_ft)
            AS rnk
    FROM xy_to_stadium_dist
)

-- Choose closest stadium per parcel
SELECT
    pin10,
    stadium_name AS nearest_stadium_name,
    stadium_dist_ft AS nearest_stadium_dist_ft,
    year AS nearest_stadium_data_year,
    year
FROM min_stadium_dist
WHERE rnk = 1
