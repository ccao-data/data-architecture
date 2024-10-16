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
    WHERE road_type = 'Freeway And Expressway'
),

traffic_major_collector AS (  -- noqa: ST03
    SELECT *
    FROM {{ source('spatial', 'traffic') }}
    WHERE road_type = 'Major Collector'
),

distinct_pins AS (
    SELECT DISTINCT
        x_3435,
        y_3435,
        pin10
    FROM {{ source('spatial', 'parcel') }}
),

distinct_years_rhs AS (
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
        ARBITRARY(xy.surface_width) AS nearest_minor_road_surface_width
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
        ARBITRARY(xy.surface_width) AS nearest_interstate_road_surface_width
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
        ARBITRARY(xy.surface_width) AS nearest_freeway_road_surface_width
    FROM distinct_pins AS pcl
    INNER JOIN ( {{ dist_to_nearest_geometry('traffic_freeway') }} ) AS xy
        ON pcl.x_3435 = xy.x_3435
        AND pcl.y_3435 = xy.y_3435
    GROUP BY pcl.pin10, xy.year
),

nearest_minor_collector AS (
    SELECT
        pcl.pin10,
        xy.year,
        ARBITRARY(xy.road_name) AS nearest_minor_collector_road_name,
        ARBITRARY(xy.dist_ft) AS nearest_minor_collector_road_dist_ft,
        ARBITRARY(xy.year) AS nearest_minor_collector_road_data_year,
        ARBITRARY(xy.surface_width)
            AS nearest_minor_collector_road_surface_width
    FROM distinct_pins AS pcl
    INNER JOIN
        ( {{ dist_to_nearest_geometry('traffic_minor_collector') }} ) AS xy
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
        ARBITRARY(xy.surface_width)
            AS nearest_major_collector_road_surface_width
    FROM distinct_pins AS pcl
    INNER JOIN
        ( {{ dist_to_nearest_geometry('traffic_major_collector') }} ) AS xy
        ON pcl.x_3435 = xy.x_3435
        AND pcl.y_3435 = xy.y_3435
    GROUP BY pcl.pin10, xy.year
)

-- Join all nearest roads by pin10 and year
SELECT
    COALESCE(
        minor.pin10, interstate.pin10, freeway.pin10, major_collector.pin10,
        minor_collector.pin10
    ) AS pin10,
    minor.nearest_minor_road_name,
    minor.nearest_minor_road_dist_ft,
    minor.nearest_minor_road_data_year,
    minor.nearest_minor_road_surface_width,
    interstate.nearest_interstate_road_name,
    interstate.nearest_interstate_road_dist_ft,
    interstate.nearest_interstate_road_data_year,
    interstate.nearest_interstate_road_surface_width,
    freeway.nearest_freeway_road_name,
    freeway.nearest_freeway_road_dist_ft,
    freeway.nearest_freeway_road_data_year,
    freeway.nearest_freeway_road_surface_width,
    major_collector.nearest_major_collector_road_name,
    major_collector.nearest_major_collector_road_dist_ft,
    major_collector.nearest_major_collector_road_data_year,
    major_collector.nearest_major_collector_road_surface_width,
    minor_collector.nearest_minor_collector_road_name,
    minor_collector.nearest_minor_collector_road_dist_ft,
    minor_collector.nearest_minor_collector_road_data_year,
    minor_collector.nearest_minor_collector_road_surface_width,
    COALESCE(
        minor.year, interstate.year, freeway.year, major_collector.year,
        minor_collector.year
    ) AS year
FROM nearest_minor AS minor
FULL OUTER JOIN nearest_interstate AS interstate
    ON minor.pin10 = interstate.pin10 AND minor.year = interstate.year
FULL OUTER JOIN nearest_freeway AS freeway
    ON COALESCE(minor.pin10, interstate.pin10) = freeway.pin10
    AND COALESCE(minor.year, interstate.year) = freeway.year
FULL OUTER JOIN nearest_major_collector AS major_collector
    ON COALESCE(minor.pin10, interstate.pin10, freeway.pin10)
    = major_collector.pin10
    AND COALESCE(minor.year, interstate.year, freeway.year)
    = major_collector.year
FULL OUTER JOIN nearest_minor_collector AS minor_collector
    ON COALESCE(
        minor.pin10, interstate.pin10, freeway.pin10, major_collector.pin10
    )
    = minor_collector.pin10
    AND COALESCE(
        minor.year, interstate.year, freeway.year, major_collector.year
    )
    = minor_collector.year
WHERE COALESCE(
        minor.year, interstate.year, freeway.year, major_collector.year,
        minor_collector.year
    ) >= (SELECT MIN(year) FROM distinct_years_rhs)
