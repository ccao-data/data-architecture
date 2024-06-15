-- CTAS to create a table of distance to the nearest Metra stop for each PIN
{{
    config(
        materialized='table',
        partitioned_by=['year'],
        bucketed_by=['pin10'],
        bucket_count=1
    )
}}

WITH stadium AS (  -- noqa: ST03
    SELECT
        ST_ASBINARY(ST_POINT(stadium.x_3435, stadium.y_3435)) AS geometry_3435,
        stadium.name,
        stadium.date_opened,
        stadium.year
    FROM {{ ref('spatial.stadium') }} AS stadium
)

SELECT
    pcl.pin10,
    ARBITRARY(xy.name) AS nearest_stadium_name,
    ARBITRARY(xy.dist_ft) AS nearest_stadium_dist_ft,
    ARBITRARY(xy.year) AS nearest_stadium_data_year,
    ARBITRARY(xy.date_opened) AS nearest_stadium_date_opened,
    pcl.year
FROM {{ source('spatial', 'parcel') }} AS pcl
INNER JOIN (
    SELECT
        st.geometry_3435,
        st.name,
        st.year,
        st.date_opened,
        pcl.x_3435,
        pcl.y_3435,
        ST_DISTANCE(
            ST_SETSRID(ST_MAKEPOINT(pcl.x_3435, pcl.y_3435), 3435),
            ST_SETSRID(st.geometry_3435, 3435)
        ) AS dist_ft
    FROM stadium AS st
    CROSS JOIN {{ source('spatial', 'parcel') }} AS pcl
) AS xy
    ON pcl.x_3435 = xy.x_3435
    AND pcl.y_3435 = xy.y_3435
GROUP BY pcl.pin10, pcl.year;
