WITH stadium AS (
    SELECT * FROM {{ ref('spatial.stadium') }}
),

-- Calculate distance between every distinct parcel and each stadium
xy_to_stadium_dist AS (
    SELECT DISTINCT
        parcel.pin10,
        parcel.x_3435,
        parcel.y_3435,
        parcel.year,
        stadium.name AS stadium_name,
        ST_DISTANCE(
            ST_POINT(parcel.x_3435, parcel.y_3435),
            ST_POINT(stadium.lon, stadium.lat)
        ) AS stadium_dist_ft
    FROM spatial.parcel
    CROSS JOIN stadium
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
            OVER (PARTITION BY pin10, year ORDER BY stadium_dist_ft) AS rnk
    FROM xy_to_stadium_dist
)

-- Choose closest stadium per parcel
SELECT
    pin10,
    stadium_name AS nearest_stadium_name,
    stadium_dist_ft AS nearest_stadium_dist_ft,
    year AS nearest_stadium_data_year
FROM min_stadium_dist
WHERE rnk = 1
