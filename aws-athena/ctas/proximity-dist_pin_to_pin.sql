-- CTAS that finds the 3 nearest neighbor PINs for every PIN for every year
-- within a 1km radius
{{
    config(
        materialized='table',
        partitioned_by=['year'],
        bucketed_by=['pin10'],
        bucket_count=1
    )
}}

WITH neighbors AS (
    {{
        nearest_neighbors(
            source("spatial", "parcel"),
            "pin10",
            3,
            [1000, 10000],
            True
        )
    }}
)

SELECT
    pcl.pin10,
    {% for idx in range(1, 4) %}
        CASE
            WHEN nghbr.rank = {{ idx + 1 }} THEN nghbr.neighbor_id
        END AS nearest_neighbor_{{ idx }}_pin10,
        CASE
            WHEN nghbr.rank = {{ idx + 1 }} THEN nghbr.distance
        END AS nearest_neighbor_{{ idx }}_dist_ft,
    {% endfor %}
    pcl.year
FROM {{ source("spatial", "parcel") }} AS pcl
INNER JOIN neighbors AS nghbr
    ON pcl.pin10 = nghbr.pin10
    AND pcl.year = nghbr.year
WHERE
    {% for idx in range(1, 4) %}
        {% if idx != 1 %} AND{% endif %}
        nearest_neighbor_{{ idx }}_pin10 IS NOT NULL
    {% endfor %}
