-- CTAS to create a table of distance to the nearest university for each PIN
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
    ARBITRARY(xy.name) AS nearest_university_name,
    ARBITRARY(xy.dist_ft) AS nearest_university_dist_ft,
    ARBITRARY(xy.year) AS nearest_university_data_year,
    pcl.year
FROM {{ source('spatial', 'parcel') }} AS pcl
INNER JOIN
    ( {{ dist_to_nearest_geometry(source('spatial', 'school_location')) }} )
        AS xy
    ON pcl.x_3435 = xy.x_3435
    AND pcl.y_3435 = xy.y_3435
    AND pcl.year = xy.pin_year
WHERE xy.type = 'HigherEd'
AND xy.name IN ('Illinois Institute of Technology Chicago-Kent College of Law', 'Columbia College', 
'University of Illinois at Chicago College of Medicine', 'Rush University Medical Center', 'University of Chicago', 
'University of Illinois at Chicago', 'Northwestern University', 'Loyola University of Chicago', 'De Paul University', 
'Chicago State University', 'Moraine Valley Community College')
GROUP BY pcl.pin10, pcl.year
