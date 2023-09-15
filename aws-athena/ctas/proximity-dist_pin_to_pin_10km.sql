-- CTAS that finds the 3 nearest neighbor PINs for every PIN for every year
-- within a 10km radius, filtered for PINs that do not have three neighbors
-- within a 1km radius
{{
    config(
        materialized='table',
        partitioned_by=['year'],
        bucketed_by=['pin10'],
        bucket_count=1
    )
}}

SELECT *
FROM (
    {{
        nearest_pin_neighbors(
            ref('proximity.dist_pin_to_pin_1km_missing_matches'),
            3,
            10000
        )
    }}
)
