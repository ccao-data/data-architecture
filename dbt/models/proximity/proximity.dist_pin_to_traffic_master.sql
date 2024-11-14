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
        AND daily_traffic IS NOT NULL
),

traffic_collector AS (  -- noqa: ST03
    SELECT *
    FROM {{ source('spatial', 'traffic') }}
    WHERE road_type = 'Major Collector'
        OR road_type = 'Minor Collector'
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
        ARBITRARY(xy.surface_type) AS nearest_minor_road_surface_type,
        ARBITRARY(xy.lanes) AS nearest_minor_road_lanes
    FROM distinct_pins AS pcl
    INNER JOIN ( {{ dist_to_nearest_geometry('traffic_minor') }} ) AS xy
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
        ARBITRARY(xy.daily_traffic) AS nearest_interstate_road_daily_traffic,
        ARBITRARY(xy.speed_limit) AS nearest_interstate_road_speed_limit,
        ARBITRARY(xy.surface_type) AS nearest_interstate_road_surface_type,
        ARBITRARY(xy.lanes) AS nearest_interstate_road_lanes
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
        ARBITRARY(xy.daily_traffic) AS nearest_freeway_road_daily_traffic,
        ARBITRARY(xy.speed_limit) AS nearest_freeway_road_speed_limit,
        ARBITRARY(xy.surface_type) AS nearest_freeway_road_surface_type,
        ARBITRARY(xy.lanes) AS nearest_freeway_road_lanes
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
        ARBITRARY(xy.daily_traffic) AS nearest_local_road_daily_traffic,
        ARBITRARY(xy.speed_limit) AS nearest_local_road_speed_limit,
        ARBITRARY(xy.surface_type) AS nearest_local_road_surface_type,
        ARBITRARY(xy.lanes) AS nearest_local_road_lanes
    FROM distinct_pins AS pcl
    INNER JOIN ( {{ dist_to_nearest_geometry('traffic_local') }} ) AS xy
        ON pcl.x_3435 = xy.x_3435
        AND pcl.y_3435 = xy.y_3435
    GROUP BY pcl.pin10, xy.year
),

-- Calculate nearest Major Collector road per pin
nearest_collector AS (
    SELECT
        pcl.pin10,
        xy.year,
        ARBITRARY(xy.road_name) AS nearest_collector_road_name,
        ARBITRARY(xy.dist_ft) AS nearest_collector_road_dist_ft,
        ARBITRARY(xy.year) AS nearest_collector_road_data_year,
        ARBITRARY(xy.speed_limit)
            AS nearest_collector_road_speed_limit,
        ARBITRARY(xy.daily_traffic) AS nearest_collector_road_daily_traffic,
        ARBITRARY(xy.surface_type) AS nearest_collector_road_surface_type,
        ARBITRARY(xy.lanes) AS nearest_collector_road_lanes
    FROM distinct_pins AS pcl
    INNER JOIN
        ( {{ dist_to_nearest_geometry('traffic_collector') }} ) AS xy
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
        ARBITRARY(xy.daily_traffic) AS nearest_other_road_daily_traffic,
        ARBITRARY(xy.speed_limit) AS nearest_other_road_speed_limit,
        ARBITRARY(xy.surface_type) AS nearest_other_road_surface_type,
        ARBITRARY(xy.lanes) AS nearest_other_road_lanes
    FROM distinct_pins AS pcl
    INNER JOIN ( {{ dist_to_nearest_geometry('traffic_other') }} ) AS xy
        ON pcl.x_3435 = xy.x_3435
        AND pcl.y_3435 = xy.y_3435
    GROUP BY pcl.pin10, xy.year
),

-- Join all nearest roads by pin10 and year
final_aggregation AS (
    SELECT
        COALESCE(
            minor.pin10, interstate.pin10, freeway.pin10,
            local_road.pin10, collector.pin10
        ) AS pin10,
        minor.nearest_minor_road_name,
        minor.nearest_minor_road_dist_ft,
        minor.nearest_minor_road_data_year,
        minor.nearest_minor_road_daily_traffic,
        minor.nearest_minor_road_speed_limit,
        minor.nearest_minor_road_surface_type,
        minor.nearest_minor_road_lanes,
        interstate.nearest_interstate_road_name,
        interstate.nearest_interstate_road_dist_ft,
        interstate.nearest_interstate_road_data_year,
        interstate.nearest_interstate_road_daily_traffic,
        interstate.nearest_interstate_road_speed_limit,
        interstate.nearest_interstate_road_surface_type,
        interstate.nearest_interstate_road_lanes,
        freeway.nearest_freeway_road_name,
        freeway.nearest_freeway_road_dist_ft,
        freeway.nearest_freeway_road_data_year,
        freeway.nearest_freeway_road_daily_traffic,
        freeway.nearest_freeway_road_speed_limit,
        freeway.nearest_freeway_road_surface_type,
        freeway.nearest_freeway_road_lanes,
        local_road.nearest_local_road_name,
        local_road.nearest_local_road_dist_ft,
        local_road.nearest_local_road_data_year,
        local_road.nearest_local_road_daily_traffic,
        local_road.nearest_local_road_speed_limit,
        local_road.nearest_local_road_surface_type,
        local_road.nearest_local_road_lanes,
        collector.nearest_collector_road_name,
        collector.nearest_collector_road_dist_ft,
        collector.nearest_collector_road_data_year,
        collector.nearest_collector_road_daily_traffic,
        collector.nearest_collector_road_speed_limit,
        collector.nearest_collector_road_surface_type,
        collector.nearest_collector_road_lanes,
        other.nearest_other_road_name,
        other.nearest_other_road_dist_ft,
        other.nearest_other_road_data_year,
        other.nearest_other_road_daily_traffic,
        other.nearest_other_road_speed_limit,
        other.nearest_other_road_surface_type,
        other.nearest_other_road_lanes,
        COALESCE(
            minor.year, interstate.year, freeway.year,
            local_road.year, collector.year, other.year)
            AS year
    FROM nearest_minor AS minor
    FULL OUTER JOIN nearest_interstate AS interstate
        ON minor.pin10 = interstate.pin10
        AND minor.year = interstate.year
    FULL OUTER JOIN nearest_freeway AS freeway
        ON COALESCE(minor.pin10, interstate.pin10) = freeway.pin10
        AND COALESCE(minor.year, interstate.year) = freeway.year
    FULL OUTER JOIN nearest_local AS local_road
        ON COALESCE(minor.pin10, interstate.pin10, freeway.pin10)
        = local_road.pin10
        AND COALESCE(minor.year, interstate.year, freeway.year)
        = local_road.year
    FULL OUTER JOIN nearest_collector AS collector
        ON COALESCE(
            minor.pin10, interstate.pin10,
            freeway.pin10, local_road.pin10
        )
        = collector.pin10
        AND COALESCE(minor.year, interstate.year, freeway.year, local_road.year)
        = collector.year
    FULL OUTER JOIN nearest_other AS other
        ON COALESCE(
            minor.pin10,
            interstate.pin10,
            freeway.pin10,
            local_road.pin10,
            collector.pin10
        )
        = other.pin10
        AND COALESCE(
            minor.year,
            interstate.year,
            freeway.year,
            local_road.year,
            collector.year
        )
        = other.year
    WHERE COALESCE(
            minor.year, interstate.year, freeway.year,
            local_road.year, collector.year,
            other.year
        )
        >= (SELECT MIN(year) FROM "awsdatacatalog"."spatial"."traffic"
    WHERE road_type IS NOT NULL)
)


SELECT
    pin10,
    nearest_minor_road_name,
    nearest_minor_road_dist_ft,
    nearest_minor_road_data_year,
    nearest_minor_road_daily_traffic,
    nearest_minor_road_speed_limit,
    nearest_minor_road_surface_type,
    nearest_minor_road_lanes,
    nearest_interstate_road_name,
    nearest_interstate_road_dist_ft,
    nearest_interstate_road_data_year,
    nearest_interstate_road_daily_traffic,
    nearest_interstate_road_speed_limit,
    nearest_interstate_road_surface_type,
    nearest_interstate_road_lanes,
    nearest_freeway_road_name,
    nearest_freeway_road_dist_ft,
    nearest_freeway_road_data_year,
    nearest_freeway_road_daily_traffic,
    nearest_freeway_road_speed_limit,
    nearest_freeway_road_surface_type,
    nearest_freeway_road_lanes,
    nearest_local_road_name,
    nearest_local_road_dist_ft,
    nearest_local_road_data_year,
    nearest_local_road_daily_traffic,
    nearest_local_road_speed_limit,
    nearest_local_road_surface_type,
    nearest_local_road_lanes,
    nearest_collector_road_name,
    nearest_collector_road_dist_ft,
    nearest_collector_road_data_year,
    nearest_collector_road_daily_traffic,
    nearest_collector_road_speed_limit,
    nearest_collector_road_surface_type,
    nearest_collector_road_lanes,
    nearest_other_road_name,
    nearest_other_road_dist_ft,
    nearest_other_road_data_year,
    nearest_other_road_daily_traffic,
    nearest_other_road_speed_limit,
    nearest_other_road_surface_type,
    nearest_other_road_lanes,
    {{ nearest_feature_aggregation(
        [
            "nearest_minor_road_dist_ft", 
            "nearest_interstate_road_dist_ft", 
            "nearest_other_road_dist_ft",
            "nearest_freeway_road_dist_ft",
            "nearest_local_road_dist_ft",
            "nearest_collector_road_dist_ft"
        ],
        [
            "name", 
            "dist_ft", 
            "data_year", 
            "daily_traffic", 
            "speed_limit", 
            "surface_type", 
            "lanes"
        ]
    ) }}
    year
FROM final_aggregation
