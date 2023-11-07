-- CTAS to create a table of distance to the nearest rail tracks for each PIN
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
        x_3435,
        y_3435
    FROM {{ source('spatial', 'parcel') }}
),

distinct_years AS (
    SELECT DISTINCT year
    FROM {{ source('spatial', 'parcel') }}
),

railroad_location AS (
    SELECT
        fill_years.pin_year,
        fill_data.*
    FROM (
        SELECT
            dy.year AS pin_year,
            MAX(df.year) AS fill_year
        FROM {{ source('spatial', 'railroad') }} AS df
        CROSS JOIN distinct_years AS dy
        WHERE dy.year >= df.year
        GROUP BY dy.year
    ) AS fill_years
    LEFT JOIN {{ source('spatial', 'railroad') }} AS fill_data
        ON fill_years.fill_year = fill_data.year
),

railroad_location_agg AS (
    SELECT
        dy.year AS pin_year,
        MAX(df.year) AS fill_year,
        geometry_union_agg(ST_GEOMFROMBINARY(df.geometry_3435)) AS geom_3435
    FROM {{ source('spatial', 'railroad') }} AS df
    CROSS JOIN distinct_years AS dy
    WHERE dy.year >= df.year
    GROUP BY dy.year
),

nearest_point AS (
    SELECT
        dp.x_3435,
        dp.y_3435,
        geometry_nearest_points(
            ST_POINT(dp.x_3435, dp.y_3435),
            loc_agg.geom_3435
        ) AS points
    FROM distinct_pins AS dp
    CROSS JOIN railroad_location_agg AS loc_agg
),

xy_to_railroad_dist AS (
    SELECT
        np.x_3435,
        np.y_3435,
        loc.name_id,
        loc.name_anno,
        loc.pin_year,
        loc.year,
        ST_Distance(np.points[1], np.points[2]) AS dist_ft
    FROM nearest_point AS np
    LEFT JOIN railroad_location AS loc
        ON ST_INTERSECTS(np.points[2], ST_GEOMFROMBINARY(loc.geometry_3435))
)

SELECT
    pcl.pin10,
    ARBITRARY(xy.name_id) AS nearest_railroad_id,
    ARBITRARY(xy.name_anno) AS nearest_railroad_name,
    ARBITRARY(xy.dist_ft) AS nearest_railroad_dist_ft,
    ARBITRARY(xy.year) AS nearest_railroad_data_year,
    pcl.year
FROM {{ source('spatial', 'parcel') }} AS pcl
INNER JOIN xy_to_railroad_dist AS xy
    ON pcl.x_3435 = xy.x_3435
    AND pcl.y_3435 = xy.y_3435
    AND pcl.year = xy.pin_year
GROUP BY pcl.pin10, pcl.year
