SELECT
    name,
    ST_ASBINARY(ST_POINT(lon, lat)) AS geometry,
    ST_ASBINARY(ST_POINT(x_3435, y_3435)) AS geometry_3435,
    CAST(year AS VARCHAR) AS year
FROM {{ ref('spatial.stadium_raw') }}
