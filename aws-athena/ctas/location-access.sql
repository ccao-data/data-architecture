{{
    config(
        materialized='table',
        partitioned_by=['year'],
        bucketed_by=['pin10'],
        bucket_count=1
    )
}}

WITH distinct_pins AS (
    SELECT DISTINCT
        x_3435,
        y_3435
    FROM {{ source('spatial', 'parcel') }}
),

distinct_years AS (
    SELECT DISTINCT year
    FROM {{ source('spatial', 'parcel') }}
),

distinct_years_rhs AS (
    SELECT DISTINCT year
    FROM {{ source('spatial', 'walkability') }}
),

walkability AS (
    SELECT
        dp.x_3435,
        dp.y_3435,
        CAST(CAST(MAX(cprod.walk_num) AS BIGINT) AS VARCHAR)
            AS access_cmap_walk_id,
        MAX(cprod.nta_score) AS access_cmap_walk_nta_score,
        MAX(cprod.total_score) AS access_cmap_walk_total_score,
        MAX(cprod.year) AS access_cmap_walk_data_year,
        cprod.pin_year
    FROM distinct_pins AS dp
    LEFT JOIN (
        SELECT
            fill_years.pin_year,
            fill_data.*
        FROM (
            SELECT
                dy.year AS pin_year,
                MAX(df.year) AS fill_year
            FROM {{ source('spatial', 'walkability') }} AS df
            CROSS JOIN distinct_years AS dy
            WHERE dy.year >= df.year
            GROUP BY dy.year
        ) AS fill_years
        LEFT JOIN {{ source('spatial', 'walkability') }} AS fill_data
            ON fill_years.fill_year = fill_data.year
    ) AS cprod
        ON ST_WITHIN(
            ST_POINT(dp.x_3435, dp.y_3435),
            ST_GEOMFROMBINARY(cprod.geometry_3435)
        )
    GROUP BY dp.x_3435, dp.y_3435, cprod.pin_year
)

SELECT
    pcl.pin10,
    walk.access_cmap_walk_id,
    walk.access_cmap_walk_nta_score,
    walk.access_cmap_walk_total_score,
    walk.access_cmap_walk_data_year,
    pcl.year
FROM {{ source('spatial', 'parcel') }} AS pcl
LEFT JOIN walkability AS walk
    ON pcl.x_3435 = walk.x_3435
    AND pcl.y_3435 = walk.y_3435
    AND pcl.year = walk.pin_year
WHERE pcl.year >= (SELECT MIN(year) FROM distinct_years_rhs)
