-- CTAS that finds the 3 nearest neighbor PINs for every PIN for every year
-- within a 100 foot radius
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
            source('spatial', 'parcel'),
            3,
            100
        )
    }}
)
