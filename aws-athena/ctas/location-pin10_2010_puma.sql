{{
    config(
        materialized='table'
    )
}}

WITH distinct_pins AS (
    SELECT DISTINCT
        pin10,
        x_3435,
        y_3435
    FROM {{ source('spatial', 'parcel') }}
),

pumas AS (
    SELECT DISTINCT
        geoid,
        geometry_3435
    FROM {{ source('spatial', 'census') }}
    WHERE year = '2021'
        AND geography = 'puma'
)

SELECT DISTINCT
    dp.pin10,
    pumas.geoid AS geoid_2010
FROM distinct_pins AS dp
LEFT JOIN pumas
    ON ST_WITHIN(
        ST_POINT(dp.x_3435, dp.y_3435),
        ST_GEOMFROMBINARY(pumas.geometry_3435)
    )
