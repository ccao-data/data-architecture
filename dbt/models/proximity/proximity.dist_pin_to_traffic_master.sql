{{
    config(
        materialized='table',
        partitioned_by=['year'],
        bucketed_by=['pin10'],
        bucket_count=1
    )
}}

WITH traffic_minor AS (  -- noqa: ST03
    SELECT *
    FROM {{ source('spatial', 'traffic') }}
    WHERE road_type = 'Minor Arterial'
),

traffic_minor_collector AS (  -- noqa: ST03
    SELECT *
    FROM {{ source('spatial', 'traffic') }}
    WHERE road_type = 'Minor Collector'
),

traffic_interstate AS (  -- noqa: ST03
    SELECT *
    FROM {{ source('spatial', 'traffic') }}
    WHERE road_type = 'Interstate'
),

traffic_freeway AS (  -- noqa: ST03
    SELECT *
    FROM {{ source('spatial', 'traffic') }}
    WHERE road_type = 'Freeway and Expressway'
),

traffic_local AS (  -- noqa: ST03
    SELECT *
    FROM {{ source('spatial', 'traffic') }}
    WHERE road_type = 'Local Road or Street'
),

traffic_major_collector AS (  -- noqa: ST03
    SELECT *
    FROM {{ source('spatial', 'traffic') }}
    WHERE road_type = 'Major Collector'
),

traffic_other AS (  -- noqa: ST03
    SELECT *
    FROM {{ source('spatial', 'traffic') }}
    WHERE road_type = 'Other Principal Arterial'
),

distinct_pins AS (
    SELECT DISTINCT
        x_3435,
        y_3435,
        pin10
    FROM {{ source('spatial', 'parcel') }}
),

distinct_years AS (
    SELECT DISTINCT year
    FROM {{ source('spatial', 'traffic') }}
    WHERE road_type IS NOT NULL
),

-- Calculate nearest Minor Arterial road per pin
nearest_minor AS (
    SELECT
        pcl.pin10,
        xy.year,
        ARBITRARY(xy.road_name) AS nearest_minor_road_name,
        ARBITRARY(xy.dist_ft) AS nearest_minor_road_dist_ft,
        ARBITRARY(xy.year) AS nearest_minor_road_data_year,
        ARBITRARY(xy.daily_traffic) AS nearest_minor_road_daily_traffic,
        ARBITRARY(xy.speed_limit) AS nearest_minor_road_speed_limit,
        ARBITRARY(xy.surface_type) AS nearest_minor_surface_type,
        ARBITRARY(xy.lanes) AS nearest_minor_lanes
    FROM distinct_pins AS pcl
    INNER JOIN ( {{ dist_to_nearest_geometry('traffic_minor') }} ) AS xy
        ON pcl.x_3435 = xy.x_3435
        AND pcl.y_3435 = xy.y_3435
    GROUP BY pcl.pin10, xy.year
),

-- Calculate nearest Minor Collector road per pin
nearest_minor_collector AS (
    SELECT
        pcl.pin10,
        xy.year,
        ARBITRARY(xy.road_name) AS nearest_minor_collector_road_name,
        ARBITRARY(xy.dist_ft) AS nearest_minor_collector_road_dist_ft,
        ARBITRARY(xy.year) AS nearest_minor_collector_road_data_year,
        ARBITRARY(xy.daily_traffic) AS nearest_minor_collector_daily_traffic,
        ARBITRARY(xy.speed_limit) AS nearest_minor_collector_road_speed_limit,
        ARBITRARY(xy.surface_type) AS nearest_minor_collector_surface_type,
        ARBITRARY(xy.lanes) AS nearest_minor_collector_lanes
    FROM distinct_pins AS pcl
    INNER JOIN
        ( {{ dist_to_nearest_geometry('traffic_minor_collector') }} ) AS xy
        ON pcl.x_3435 = xy.x_3435
        AND pcl.y_3435 = xy.y_3435
    GROUP BY pcl.pin10, xy.year
),

-- Calculate nearest Interstate road per pin
nearest_interstate AS (
    SELECT
        pcl.pin10,
        xy.year,
        ARBITRARY(xy.road_name) AS nearest_interstate_road_name,
        ARBITRARY(xy.dist_ft) AS nearest_interstate_road_dist_ft,
        ARBITRARY(xy.year) AS nearest_interstate_road_data_year,
        ARBITRARY(xy.daily_traffic) AS nearest_interstate_daily_traffic,
        ARBITRARY(xy.speed_limit) AS nearest_interstate_road_speed_limit,
        ARBITRARY(xy.surface_type) AS nearest_interstate_surface_type,
        ARBITRARY(xy.lanes) AS nearest_interstate_lanes
    FROM distinct_pins AS pcl
    INNER JOIN ( {{ dist_to_nearest_geometry('traffic_interstate') }} ) AS xy
        ON pcl.x_3435 = xy.x_3435
        AND pcl.y_3435 = xy.y_3435
    GROUP BY pcl.pin10, xy.year
),

-- Calculate nearest Freeway or Expressway road per pin
nearest_freeway AS (
    SELECT
        pcl.pin10,
        xy.year,
        ARBITRARY(xy.road_name) AS nearest_freeway_road_name,
        ARBITRARY(xy.dist_ft) AS nearest_freeway_road_dist_ft,
        ARBITRARY(xy.year) AS nearest_freeway_road_data_year,
        ARBITRARY(xy.daily_traffic) AS nearest_freeway_daily_traffic,
        ARBITRARY(xy.speed_limit) AS nearest_freeway_road_speed_limit,
        ARBITRARY(xy.surface_type) AS nearest_freeway_surface_type,
        ARBITRARY(xy.lanes) AS nearest_freeway_lanes
    FROM distinct_pins AS pcl
    INNER JOIN ( {{ dist_to_nearest_geometry('traffic_freeway') }} ) AS xy
        ON pcl.x_3435 = xy.x_3435
        AND pcl.y_3435 = xy.y_3435
    GROUP BY pcl.pin10, xy.year
),

nearest_local AS (
    SELECT
        pcl.pin10,
        xy.year,
        ARBITRARY(xy.road_name) AS nearest_local_road_name,
        ARBITRARY(xy.dist_ft) AS nearest_local_road_dist_ft,
        ARBITRARY(xy.year) AS nearest_local_road_data_year,
        ARBITRARY(xy.daily_traffic) AS nearest_local_daily_traffic,
        ARBITRARY(xy.speed_limit) AS nearest_local_road_speed_limit,
        ARBITRARY(xy.surface_type) AS nearest_local_surface_type,
        ARBITRARY(xy.lanes) AS nearest_local_lanes
    FROM distinct_pins AS pcl
    INNER JOIN ( {{ dist_to_nearest_geometry('traffic_local') }} ) AS xy
        ON pcl.x_3435 = xy.x_3435
        AND pcl.y_3435 = xy.y_3435
    GROUP BY pcl.pin10, xy.year
),

-- Calculate nearest Major Collector road per pin
nearest_major_collector AS (
    SELECT
        pcl.pin10,
        xy.year,
        ARBITRARY(xy.road_name) AS nearest_major_collector_road_name,
        ARBITRARY(xy.dist_ft) AS nearest_major_collector_road_dist_ft,
        ARBITRARY(xy.year) AS nearest_major_collector_road_data_year,
        ARBITRARY(xy.speed_limit)
            AS nearest_major_collector_road_speed_limit,
        ARBITRARY(xy.daily_traffic) AS nearest_major_collector_daily_traffic,
        ARBITRARY(xy.surface_type) AS nearest_major_collector_surface_type,
        ARBITRARY(xy.lanes) AS nearest_major_collector_lanes
    FROM distinct_pins AS pcl
    INNER JOIN
        ( {{ dist_to_nearest_geometry('traffic_major_collector') }} ) AS xy
        ON pcl.x_3435 = xy.x_3435
        AND pcl.y_3435 = xy.y_3435
    GROUP BY pcl.pin10, xy.year
),

-- Calculate nearest Other Principal Arterial road per pin
nearest_other AS (
    SELECT
        pcl.pin10,
        xy.year,
        ARBITRARY(xy.road_name) AS nearest_other_road_name,
        ARBITRARY(xy.dist_ft) AS nearest_other_road_dist_ft,
        ARBITRARY(xy.year) AS nearest_other_road_data_year,
        ARBITRARY(xy.daily_traffic) AS nearest_other_daily_traffic,
        ARBITRARY(xy.speed_limit) AS nearest_other_road_speed_limit,
        ARBITRARY(xy.surface_type) AS nearest_other_surface_type,
        ARBITRARY(xy.lanes) AS nearest_other_lanes
    FROM distinct_pins AS pcl
    INNER JOIN ( {{ dist_to_nearest_geometry('traffic_other') }} ) AS xy
        ON pcl.x_3435 = xy.x_3435
        AND pcl.y_3435 = xy.y_3435
    GROUP BY pcl.pin10, xy.year
)

-- Join all nearest roads by pin10 and year
SELECT
    COALESCE(
        minor.pin10, minor_collector.pin10, interstate.pin10, freeway.pin10,
        local_road.pin10, major_collector.pin10
    ) AS pin10,
    minor.nearest_minor_road_name,
    minor.nearest_minor_road_dist_ft,
    minor.nearest_minor_road_data_year,
    minor.nearest_minor_road_daily_traffic,
    minor.nearest_minor_road_speed_limit,
    minor.nearest_minor_surface_type,
    minor.nearest_minor_lanes,
    minor_collector.nearest_minor_collector_road_name,
    minor_collector.nearest_minor_collector_road_dist_ft,
    minor_collector.nearest_minor_collector_road_data_year,
    minor_collector.nearest_minor_collector_daily_traffic,
    minor_collector.nearest_minor_collector_road_speed_limit,
    minor_collector.nearest_minor_collector_surface_type,
    minor_collector.nearest_minor_collector_lanes,
    interstate.nearest_interstate_road_name,
    interstate.nearest_interstate_road_dist_ft,
    interstate.nearest_interstate_road_data_year,
    interstate.nearest_interstate_daily_traffic,
    interstate.nearest_interstate_road_speed_limit,
    interstate.nearest_interstate_surface_type,
    interstate.nearest_interstate_lanes,
    freeway.nearest_freeway_road_name,
    freeway.nearest_freeway_road_dist_ft,
    freeway.nearest_freeway_road_data_year,
    freeway.nearest_freeway_daily_traffic,
    freeway.nearest_freeway_road_speed_limit,
    freeway.nearest_freeway_surface_type,
    freeway.nearest_freeway_lanes,
    local_road.nearest_local_road_name,
    local_road.nearest_local_road_dist_ft,
    local_road.nearest_local_road_data_year,
    local_road.nearest_local_daily_traffic,
    local_road.nearest_local_road_speed_limit,
    local_road.nearest_local_surface_type,
    local_road.nearest_local_lanes,
    major_collector.nearest_major_collector_road_name,
    major_collector.nearest_major_collector_road_dist_ft,
    major_collector.nearest_major_collector_road_data_year,
    major_collector.nearest_major_collector_daily_traffic,
    major_collector.nearest_major_collector_road_speed_limit,
    major_collector.nearest_major_collector_surface_type,
    major_collector.nearest_major_collector_lanes,
    other.nearest_other_road_name,
    other.nearest_other_road_dist_ft,
    other.nearest_other_road_data_year,
    other.nearest_other_daily_traffic,
    other.nearest_other_road_speed_limit,
    other.nearest_other_surface_type,
    other.nearest_other_lanes,
    COALESCE(
        minor.year, minor_collector.year interstate.year, freeway.year,
        local_road.year, major_collector.year, other.year)
        AS year
FROM nearest_minor AS minor
FULL OUTER JOIN nearest_interstate AS interstate
    ON minor.pin10 = interstate.pin10 AND minor.year = interstate.year
FULL OUTER JOIN nearest_freeway AS freeway
    ON COALESCE(minor.pin10, interstate.pin10) = freeway.pin10
    AND COALESCE(minor.year, interstate.year) = freeway.year
FULL OUTER JOIN nearest_local AS local_road
    ON COALESCE(minor.pin10, interstate.pin10, freeway.pin10)
    = local_road.pin10
    AND COALESCE(minor.year, interstate.year, freeway.year)
    = local_road.year
FULL OUTER JOIN nearest_major_collector AS major_collector
    ON COALESCE(minor.pin10, interstate.pin10, freeway.pin10, local_road.pin10)
    = major_collector.pin10
    AND COALESCE(minor.year, interstate.year, freeway.year, local_road.year)
    = major_collector.year
FULL OUTER JOIN nearest_other AS other
    ON COALESCE(
        minor.pin10,
        interstate.pin10,
        freeway.pin10,
        local_road.pin10,
        major_collector.pin10
    )
    = other.pin10
    AND COALESCE(
        minor.year,
        interstate.year,
        freeway.year,
        local_road.year,
        major_collector.year
    )
    = other.year
FULL OUTER JOIN nearest_minor_collector AS minor_collector
    ON COALESCE(
        minor.pin10, interstate.pin10, freeway.pin10,
        local_road.pin10, major_collector.pin10, other.pin10
    )
    = minor_collector.pin10
    AND COALESCE(
        minor.year, interstate.year, freeway.year,
        local_road.year, major_collector.year, other.year
    )
    = minor_collector.year
WHERE COALESCE(
        minor.year, minor_collector.year, interstate.year, freeway.year,
        local_road.year, major_collector.year,
        other.year
    )
    >= (SELECT MIN(year) FROM distinct_years)
