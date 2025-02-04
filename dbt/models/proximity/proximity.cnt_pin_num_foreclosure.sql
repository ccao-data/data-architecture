-- CTAS to create a table of foreclosure counts per PIN. Counts are within 1/2
-- mile and past 5 years of each target PIN
{{
    config(
        materialized='table',
        partitioned_by=['year'],
        bucketed_by=['pin10'],
        bucket_count=1
    )
}}

WITH pin_locations AS (
    SELECT
        pin10,
        year,
        x_3435,
        y_3435,
        ST_POINT(x_3435, y_3435) AS point
    FROM {{ source('spatial', 'parcel') }}
),

distinct_pins AS (
    SELECT DISTINCT
        x_3435,
        y_3435
    FROM pin_locations
),

foreclosure_locations AS (
    SELECT
        *,
        ST_BUFFER(ST_GEOMFROMBINARY(geometry_3435), 2640) AS buffer
    FROM sale.foreclosure
),

pins_in_buffers AS (
    SELECT
        pl.pin10,
        pl.year,
        COUNT(*) AS num_foreclosure_in_half_mile_past_5_years
    FROM pin_locations AS pl
    INNER JOIN foreclosure_locations AS loc
        ON YEAR(loc.foreclosure_recording_date)
        BETWEEN CAST(pl.year AS INT) - 5 AND CAST(pl.year AS INT)
        AND ST_CONTAINS(loc.buffer, pl.point)
    GROUP BY pl.pin10, pl.year
),

pin_counts_in_half_mile AS (
    SELECT
        dp.x_3435,
        dp.y_3435,
        pl.year,
        COUNT(*) AS num_pin_in_half_mile
    FROM distinct_pins AS dp
    INNER JOIN pin_locations AS pl
        ON ST_CONTAINS(
            ST_BUFFER(ST_POINT(dp.x_3435, dp.y_3435), 2640), pl.point
        )
    GROUP BY dp.x_3435, dp.y_3435, pl.year
)

SELECT
    pl.pin10,
    COALESCE(
        pib.num_foreclosure_in_half_mile_past_5_years,
        0
    ) AS num_foreclosure_in_half_mile_past_5_years,
    COALESCE(pc.num_pin_in_half_mile, 1) AS num_pin_in_half_mile,
    CASE
        WHEN
            COALESCE(pib.num_foreclosure_in_half_mile_past_5_years, 0) = 0
            THEN 0
        ELSE ROUND(
                CAST(pib.num_foreclosure_in_half_mile_past_5_years AS DOUBLE)
                / (CAST(COALESCE(pc.num_pin_in_half_mile, 1) AS DOUBLE) / 1000),
                2
            )
    END AS num_foreclosure_per_1000_pin_past_5_years,
    CONCAT(
        CAST(CAST(pl.year AS INT) - 5 AS VARCHAR),
        ' - ',
        CAST(pl.year AS VARCHAR)
    ) AS num_foreclosure_data_year,
    pl.year
FROM pin_locations AS pl
LEFT JOIN pins_in_buffers AS pib
    ON pl.pin10 = pib.pin10
    AND pl.year = pib.year
LEFT JOIN pin_counts_in_half_mile AS pc
    ON pl.x_3435 = pc.x_3435
    AND pl.y_3435 = pc.y_3435
    AND pl.year = pc.year
