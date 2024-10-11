WITH traffic_minor AS (  -- noqa: ST03
    SELECT *
    FROM {{ source('spatial', 'traffic') }}
    WHERE daily_traffic > 0
        AND daily_traffic IS NOT NULL
        AND road_type = 'Minor Arterial'
),

traffic_highway AS (  -- noqa: ST03
    SELECT *
    FROM {{ source('spatial', 'traffic') }}
    WHERE daily_traffic > 0
        AND daily_traffic IS NOT NULL
        AND (
            road_type = 'Interstate'
            OR road_type = 'Freeway And Expressway'
        )
),

distinct_pins AS (
    SELECT DISTINCT
        x_3435,
        y_3435,
        pin10,
        year
    FROM {{ source('spatial', 'parcel') }}
),

-- Select nearest road from Minor Arterial and Highway
nearest_road AS (
    SELECT
        pcl.pin10,
        pcl.year,
        xy_minor.road_name AS nearest_minor_road_name,
        xy_minor.dist_ft AS nearest_minor_road_dist_ft,
        xy_minor.daily_traffic AS nearest_minor_daily_traffic,
        xy_highway.road_name AS nearest_highway_road_name,
        xy_highway.dist_ft AS nearest_highway_road_dist_ft,
        xy_highway.daily_traffic AS nearest_highway_daily_traffic
    FROM distinct_pins AS pcl
    LEFT JOIN ( {{ dist_to_nearest_geometry('traffic_minor') }} ) AS xy_minor
        ON pcl.x_3435 = xy_minor.x_3435
        AND pcl.y_3435 = xy_minor.y_3435
        AND pcl.year = xy_minor.pin_year
    LEFT JOIN
        ( {{ dist_to_nearest_geometry('traffic_highway') }} ) AS xy_highway
        ON pcl.x_3435 = xy_highway.x_3435
        AND pcl.y_3435 = xy_highway.y_3435
        AND pcl.year = xy_highway.pin_year
)

-- Combine the data into a single row for each PIN
SELECT
    pin10,
    year,
    ARBITRARY(nearest_minor_road_name) AS nearest_minor_road_name,
    ARBITRARY(nearest_minor_road_dist_ft) AS nearest_minor_road_dist_ft,
    ARBITRARY(nearest_minor_daily_traffic) AS nearest_minor_daily_traffic,
    ARBITRARY(nearest_highway_road_name) AS nearest_highway_road_name,
    ARBITRARY(nearest_highway_road_dist_ft) AS nearest_highway_road_dist_ft,
    ARBITRARY(nearest_highway_daily_traffic) AS nearest_highway_daily_traffic
FROM nearest_road
GROUP BY pin10, year
