-- CTAS to create a table of distance to the nearest hospital for each PIN
{{
    config(
        materialized='table',
        partitioned_by=['year'],
        bucketed_by=['pin10'],
        bucket_count=1
    )
}}

SELECT
    pcl.pin10,
    CAST(CAST(ARBITRARY(xy.gniscode) AS BIGINT) AS VARCHAR)
        AS nearest_university_gnis_code,
    ARBITRARY(xy.name) AS nearest_university_name,
    ARBITRARY(xy.dist_ft) AS nearest_university_dist_ft,
    ARBITRARY(xy.year) AS nearest_university_data_year,
    pcl.year
FROM {{ source('spatial', 'parcel') }} AS pcl
INNER JOIN
    (
        SELECT
            school.gniscode,
            school.name,
            ST_DISTANCE(school.geometry_3435, pcl.geometry_3435) AS dist_ft,
            school.year AS pin_year,
            ST_X(school.geometry_3435) AS x_3435,
            ST_Y(school.geometry_3435) AS y_3435
        FROM {{ source('spatial', 'school_location') }} AS school
        WHERE school.type = 'HigherEd'
    ) AS xy
    ON pcl.x_3435 = xy.x_3435
    AND pcl.y_3435 = xy.y_3435
    AND pcl.year = xy.pin_year
GROUP BY pcl.pin10, pcl.year
