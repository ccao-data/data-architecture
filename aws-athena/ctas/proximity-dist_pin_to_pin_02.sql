-- CTAS that finds the 3 nearest neighbor PINs for every PIN for every year
-- within a 500 meter radius, filtered for PINs that do not have three
-- neighbors within a 100 meter radius
{{
    config(
        materialized='table',
        partitioned_by=['year'],
        bucketed_by=['pin10'],
        bucket_count=1
    )
}}

WITH missing_matches AS (  -- noqa: ST03
    SELECT
        pcl.pin10,
        pcl.year,
        pcl.x_3435,
        pcl.y_3435,
        dist_pin_to_pin_01.pin10 AS matching_pin10
    FROM {{ source('spatial', 'parcel') }} AS pcl
    LEFT JOIN {{ ref('proximity.dist_pin_to_pin_01') }} AS dist_pin_to_pin_01
        ON pcl.pin10 = dist_pin_to_pin_01.pin10
        AND pcl.year = dist_pin_to_pin_01.year
    WHERE dist_pin_to_pin_01.pin10 IS NULL
)

SELECT *
FROM (
    {{
        nearest_pin_neighbors(
            'missing_matches',
            3,
            500
        )
    }}
)
