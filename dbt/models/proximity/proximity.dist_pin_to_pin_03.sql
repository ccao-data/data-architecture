-- CTAS that finds the 3 nearest neighbor PINs for every PIN for every year
-- within a 10,000 foot radius, filtered for PINs that do not have three
-- neighbors within a 500 foot radius
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
        pcl.y_3435
    FROM {{ source('spatial', 'parcel') }} AS pcl
    LEFT JOIN (
        SELECT * FROM {{ ref('proximity.dist_pin_to_pin_01') }}
        UNION
        SELECT * FROM {{ ref('proximity.dist_pin_to_pin_02') }}
    ) AS dist_pin_to_pin
        ON pcl.pin10 = dist_pin_to_pin.pin10
        AND pcl.year = dist_pin_to_pin.year
    WHERE dist_pin_to_pin.pin10 IS NULL
)

SELECT *
FROM (
    {{
        nearest_pin_neighbors(
            'missing_matches',
            3,
            10000
        )
    }}
)
