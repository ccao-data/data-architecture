-- CTAS to create a table of distance to the nearest university for each PIN
{{
    config(
        materialized='table',
        partitioned_by=['year'],
        bucketed_by=['pin10'],
        bucket_count=1
    )
}}

WITH major_universities AS (  -- noqa: ST03
    SELECT *
    FROM {{ source('spatial', 'school_location') }}
    WHERE type = 'HigherEd'
        AND name IN (
            'Illinois Institute of Technology Chicago-Kent College of Law',
            'Columbia College',
            'University of Illinois at Chicago College of Medicine',
            'Rush University Medical Center',
            'University of Chicago',
            'University of Illinois at Chicago',
            'Northwestern University',
            'Loyola University of Chicago',
            'Chicago State University',
            'Moraine Valley Community College'
        )
        OR gniscode IN (407022)
)

SELECT
    pcl.pin10,
    ARBITRARY(xy.name) AS nearest_university_name,
    ARBITRARY(xy.dist_ft) AS nearest_university_dist_ft,
    ARBITRARY(xy.year) AS nearest_university_data_year,
    pcl.year
FROM {{ source('spatial', 'parcel') }} AS pcl
INNER JOIN ( {{ dist_to_nearest_geometry('major_universities',
    geometry_type = "point") }} ) AS xy
    ON pcl.x_3435 = xy.x_3435
    AND pcl.y_3435 = xy.y_3435
    AND pcl.year = xy.pin_year
GROUP BY pcl.pin10, pcl.year
