-- CTAS to create a table of distance to the nearest stadium for each PIN
{{
    config(
        materialized='table',
        partitioned_by=['year'],
        bucketed_by=['pin10'],
        bucket_count=1
    )
}}

WITH stadium AS (
    SELECT * FROM {{ ref('spatial.stadium') }}
),

-- Calculate distance between every distinct parcel and each stadium
xy_to_stadium_dist AS (
    SELECT DISTINCT
        parcel.pin10,
        parcel.x_3435,
        parcel.y_3435,
        stadium.name AS stadium_name,
        ST_DISTANCE(
            ST_POINT(parcel.x_3435, parcel.y_3435),
            ST_POINT(stadium.x_3435, stadium.y_3435)
        ) AS stadium_dist_ft,
        parcel.year
    FROM spatial.parcel
    CROSS JOIN stadium
    WHERE CAST(parcel.year AS INTEGER) >= CAST(stadium.date_opened AS INTEGER)
),

-- Rank distance to stadium within parcel and year
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

-- Choose closest stadium per parcel per year
SELECT
    pin10,
    stadium_name AS nearest_stadium_name,
    stadium_dist_ft AS nearest_stadium_dist_ft,
    year
FROM min_stadium_dist
WHERE rnk = 1;
