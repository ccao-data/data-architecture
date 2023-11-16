-- CTAS to create a table of distance to the airports for each PIN
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

xy_to_airports_dist AS (
    SELECT
        dp.x_3435,
        dp.y_3435,
        ST_DISTANCE(
            ST_POINT(dp.x_3435, dp.y_3435),
            ST_POINT(1099852, 1935070) --ohare centroid
        ) AS airport_ohare_dist_ft,
        ST_DISTANCE(
            ST_POINT(dp.x_3435, dp.y_3435),
            ST_POINT(1142843, 1864827) --midway centroid
        ) AS airport_midway_dist_ft
    FROM distinct_pins AS dp
),

--Coefficients from OMP-ONLY inverse square noise falloff model for O'Hare,
-- 2019-sensor data ONLY inverse square noise falloff model for Midway
-- "Model 1" from spatial-airport-noise-point-source.R
-- (Code: https://tinyurl.com/2dp96um7)
--ohare     0.00582564978995262
--midway    0.00297149980129393

--I_ZERO = POWER(10, -12)

airport_regression AS (
    SELECT
        x_3435,
        y_3435,
        airport_ohare_dist_ft,
        airport_midway_dist_ft,
        POWER(airport_ohare_dist_ft, -2)
        * 0.00582564978995262 AS model_output_ohare,
        POWER(airport_midway_dist_ft, -2)
        * 0.00297149980129393 AS model_output_midway
    FROM xy_to_airports_dist
),

airport_modeled_dnl AS (
    SELECT
        *,
        GREATEST(
            0,
            10 * LOG(10, (model_output_ohare / POWER(10, -12)))
        ) AS airport_dnl_ohare,
        GREATEST(
            0,
            10 * LOG(10, (model_output_midway / POWER(10, -12)))
        ) AS airport_dnl_midway
    FROM airport_regression
)
SELECT
    pcl.pin10,
    dnl.airport_ohare_dist_ft,
    dnl.airport_midway_dist_ft,
    dnl.airport_dnl_ohare,
    dnl.airport_dnl_midway,
    dnl.airport_dnl_ohare + dnl.airport_dnl_midway + 50 AS airport_dnl_total,
    '2019' AS airport_data_year,
    pcl.year
FROM {{ source('spatial', 'parcel') }} AS pcl
INNER JOIN airport_modeled_dnl AS dnl
    ON pcl.x_3435 = dnl.x_3435
    AND pcl.y_3435 = dnl.y_3435
ORDER BY dnl.airport_dnl_ohare DESC
