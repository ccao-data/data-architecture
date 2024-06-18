SELECT
    stadium.name,
    ST_ASBINARY(ST_POINT(stadium.lon, stadium.lat)) AS geometry,
    ST_ASBINARY(ST_POINT(stadium.x_3435, stadium.y_3435)) AS geometry_3435,
    CAST(stadium.year AS VARCHAR) AS year
FROM {{ ref('spatial.stadium') }}
