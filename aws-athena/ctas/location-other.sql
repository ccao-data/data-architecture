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
    SELECT DISTINCT '2021' AS year
    FROM {{ source('spatial', 'subdivision') }}
    UNION ALL
    SELECT DISTINCT '2014' AS year
    FROM {{ source('spatial', 'enterprise_zone') }}
),

subdivision AS (
    SELECT
        dp.x_3435,
        dp.y_3435,
        MAX(cprod.pagesubref) AS misc_subdivision_id,
        MAX(cprod.year) AS misc_subdivision_data_year,
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
            FROM {{ source('spatial', 'subdivision') }} AS df
            CROSS JOIN distinct_years AS dy
            WHERE dy.year >= df.year
            GROUP BY dy.year
        ) AS fill_years
        LEFT JOIN {{ source('spatial', 'subdivision') }} AS fill_data
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
    sub.misc_subdivision_id,
    sub.misc_subdivision_data_year,
    pcl.year
FROM {{ source('spatial', 'parcel') }} AS pcl
LEFT JOIN subdivision AS sub
    ON pcl.x_3435 = sub.x_3435
    AND pcl.y_3435 = sub.y_3435
    AND pcl.year = sub.pin_year
WHERE pcl.year >= (SELECT MIN(year) FROM distinct_years_rhs)
