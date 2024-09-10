{{ 
    config(materialized='table') 
}}

WITH distinct_years AS (
    SELECT DISTINCT CAST(year AS INTEGER) AS year
    FROM {{ source('spatial', 'parcel') }}

),

stadium_years AS (
    SELECT
        stadium.name,
        stadium.lon,
        stadium.lat,
        stadium.x_3435,
        stadium.y_3435,
        years.year
    FROM {{ ref('spatial.stadium_raw') }} AS stadium
    CROSS JOIN distinct_years AS years
    WHERE CAST(stadium.year AS INTEGER) <= years.year
)

SELECT DISTINCT
    stadium_years.name,
    CAST(stadium_years.year AS VARCHAR) AS year,
    ST_ASBINARY(ST_POINT(stadium_years.lon, stadium_years.lat)) AS geometry,
    ST_ASBINARY(ST_POINT(stadium_years.x_3435, stadium_years.y_3435))
        AS geometry_3435
FROM stadium_years
ORDER BY year, name
