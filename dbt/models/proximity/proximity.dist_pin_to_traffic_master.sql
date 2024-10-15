-- CTAS to create a table of distance to the nearest road for each PIN
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

distinct_pins AS (
    SELECT DISTINCT
        x_3435,
        y_3435,
        pin10,
        year
    FROM {{ source('spatial', 'parcel') }}
),

-- Select nearest road from Minor Arterial
nearest_minor AS (
    SELECT
        pcl.pin10,
        ARBITRARY(xy.road_name) AS nearest_minor_road_name,
        ARBITRARY(xy.dist_ft) AS nearest_minor_road_dist_ft,
        ARBITRARY(xy.year) AS nearest_minor_road_data_year,
        ARBITRARY(xy.surface_width) AS nearest_minor_road_surface_width,
        pcl.year
    FROM distinct_pins AS pcl
    INNER JOIN ( {{ dist_to_nearest_geometry('traffic_minor') }} ) AS xy
        ON pcl.x_3435 = xy.x_3435
        AND pcl.y_3435 = xy.y_3435
        AND pcl.year = xy.pin_year
    GROUP BY pcl.pin10, pcl.year
),

-- Select nearest road from Interstate
nearest_interstate AS (
    SELECT
        pcl.pin10,
        ARBITRARY(xy.road_name) AS nearest_interstate_road_name,
        ARBITRARY(xy.dist_ft) AS nearest_interstate_road_dist_ft,
        ARBITRARY(xy.year) AS nearest_interstate_road_data_year,
        ARBITRARY(xy.surface_width) AS nearest_interstate_road_surface_width,
        pcl.year
    FROM distinct_pins AS pcl
    INNER JOIN ( {{ dist_to_nearest_geometry('traffic_interstate') }} ) AS xy
        ON pcl.x_3435 = xy.x_3435
        AND pcl.y_3435 = xy.y_3435
        AND pcl.year = xy.pin_year
    GROUP BY pcl.pin10, pcl.year
),

-- Select nearest road from Freeway And Expressway
nearest_freeway AS (
    SELECT
        pcl.pin10,
        ARBITRARY(xy.road_name) AS nearest_freeway_road_name,
        ARBITRARY(xy.dist_ft) AS nearest_freeway_road_dist_ft,
        ARBITRARY(xy.year) AS nearest_freeway_road_data_year,
        ARBITRARY(xy.surface_width) AS nearest_freeway_road_surface_width,
        pcl.year
    FROM distinct_pins AS pcl
    INNER JOIN ( {{ dist_to_nearest_geometry('traffic_freeway') }} ) AS xy
        ON pcl.x_3435 = xy.x_3435
        AND pcl.y_3435 = xy.y_3435
        AND pcl.year = xy.pin_year
    GROUP BY pcl.pin10, pcl.year
)

-- Join the results based on pin10 and year
SELECT
    COALESCE(minor.pin10, interstate.pin10, freeway.pin10) AS pin10,
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
    COALESCE(minor.year, interstate.year, freeway.year) AS year
FROM nearest_minor AS minor
FULL OUTER JOIN nearest_interstate AS interstate
    ON minor.pin10 = interstate.pin10 AND minor.year = interstate.year
FULL OUTER JOIN nearest_freeway AS freeway
    ON COALESCE(minor.pin10, interstate.pin10) = freeway.pin10
    AND COALESCE(minor.year, interstate.year) = freeway.year
