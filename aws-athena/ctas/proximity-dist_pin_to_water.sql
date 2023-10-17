-- CTAS to create a table of distance to the nearest water for each PIN
{{
    config(
        materialized='table',
        partitioned_by=['year'],
        bucketed_by=['pin10'],
        bucket_count=1
    )
}}

WITH distinct_years AS (
    SELECT DISTINCT year
    FROM {{ source('spatial', 'parcel') }}
),

water_location AS (  -- noqa: ST03
    SELECT
        fill_years.pin_year AS year,
        fill_data.year AS original_year,
        fill_data.id,
        fill_data.name,
        fill_data.hydrology_type,
        fill_data.geometry,
        fill_data.geometry_3435
    FROM (
        SELECT
            dy.year AS pin_year,
            MAX(df.year) AS fill_year
        FROM {{ source('spatial', 'hydrology') }} AS df
        CROSS JOIN distinct_years AS dy
        WHERE dy.year >= df.year
        GROUP BY dy.year
    ) AS fill_years
    LEFT JOIN {{ source('spatial', 'hydrology') }} AS fill_data
        ON fill_years.fill_year = fill_data.year
),

distances AS (
    SELECT *
    FROM (
        {{
            nearest_neighbors(
                'water_location',
                'id',
                1,
                [1000, 2000, 5000, 10000, 20000]
            )
        }}
    )
)

SELECT
    dist.pin10,
    loc.id AS nearest_water_id,
    loc.name AS nearest_water_name,
    dist.distance AS nearest_water_dist_ft,
    loc.original_year AS nearest_water_data_year,
    dist.year
FROM distances AS dist
INNER JOIN water_location AS loc
    ON dist.neighbor_id = loc.id
    AND dist.year = loc.year
