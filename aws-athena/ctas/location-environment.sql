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
    SELECT DISTINCT year FROM {{ source('spatial', 'flood_fema') }}
    UNION ALL
    SELECT DISTINCT year FROM {{ source('other', 'flood_first_street') }}
    UNION ALL
    SELECT DISTINCT year FROM {{ source('spatial', 'ohare_noise_contour') }}
    UNION ALL
    SELECT DISTINCT year FROM {{ source('other', 'airport_noise') }}
),

ohare_years AS (
    SELECT
        dy.year AS pin_year,
        MAX(df.year) AS fill_year
    FROM {{ source('spatial', 'ohare_noise_contour') }} AS df
    CROSS JOIN distinct_years AS dy
    WHERE dy.year >= df.year
    GROUP BY dy.year
),

flood_fema AS (
    SELECT
        dp.x_3435,
        dp.y_3435,
        MAX(cprod.fema_special_flood_hazard_area) AS env_flood_fema_sfha,
        MAX(cprod.year) AS env_flood_fema_data_year,
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
            FROM {{ source('spatial', 'flood_fema') }} AS df
            CROSS JOIN distinct_years AS dy
            WHERE dy.year >= df.year
            GROUP BY dy.year
        ) AS fill_years
        LEFT JOIN {{ source('spatial', 'flood_fema') }} AS fill_data
            ON fill_years.fill_year = fill_data.year
    ) AS cprod
        ON ST_WITHIN(
            ST_POINT(dp.x_3435, dp.y_3435),
            ST_GEOMFROMBINARY(cprod.geometry_3435)
        )
    GROUP BY dp.x_3435, dp.y_3435, cprod.pin_year
),

ohare_noise_contour_0000 AS (
    SELECT
        dp.x_3435,
        dp.y_3435,
        MAX(cprod.airport) AS airport,
        MAX(cprod.year) AS year,
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
            FROM {{ source('spatial', 'ohare_noise_contour') }} AS df
            CROSS JOIN distinct_years AS dy
            WHERE dy.year >= df.year
            GROUP BY dy.year
        ) AS fill_years
        LEFT JOIN
            {{ source('spatial', 'ohare_noise_contour') }} AS fill_data
            ON fill_years.fill_year = fill_data.year
    ) AS cprod
        ON ST_WITHIN(
            ST_POINT(dp.x_3435, dp.y_3435),
            ST_GEOMFROMBINARY(cprod.geometry_3435)
        )
    GROUP BY dp.x_3435, dp.y_3435, cprod.pin_year
),

ohare_noise_contour_2640 AS (
    SELECT
        dp.x_3435,
        dp.y_3435,
        MAX(cprod.airport) AS airport,
        MAX(cprod.year) AS year,
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
            FROM {{ source('spatial', 'ohare_noise_contour') }} AS df
            CROSS JOIN distinct_years AS dy
            WHERE dy.year >= df.year
            GROUP BY dy.year
        ) AS fill_years
        LEFT JOIN
            {{ source('spatial', 'ohare_noise_contour') }} AS fill_data
            ON fill_years.fill_year = fill_data.year
    ) AS cprod
        ON ST_WITHIN(
            ST_POINT(dp.x_3435, dp.y_3435),
            ST_BUFFER(ST_GEOMFROMBINARY(cprod.geometry_3435), 2640)
        )
    GROUP BY dp.x_3435, dp.y_3435, cprod.pin_year
)

SELECT
    pcl.pin10,
    flood_fema.env_flood_fema_sfha,
    flood_fema.env_flood_fema_data_year,
    flood_first_street.fs_flood_factor AS env_flood_fs_factor,
    flood_first_street.fs_flood_risk_direction
        AS env_flood_fs_risk_direction,
    flood_first_street.year AS env_flood_fs_data_year,
    CASE
        WHEN
            pcl.year >= oy.fill_year AND onc0000.airport IS NOT NULL
            THEN TRUE
        WHEN pcl.year >= oy.fill_year AND onc0000.airport IS NULL THEN FALSE
    END AS env_ohare_noise_contour_no_buffer_bool,
    CASE
        WHEN
            pcl.year >= oy.fill_year AND onc2640.airport IS NOT NULL
            THEN TRUE
        WHEN pcl.year >= oy.fill_year AND onc2640.airport IS NULL THEN FALSE
    END AS env_ohare_noise_contour_half_mile_buffer_bool,
    CASE
        WHEN pcl.year >= oy.fill_year THEN oy.fill_year
    END AS env_ohare_noise_contour_data_year,
    CASE
        WHEN pcl.year <= '2020' THEN an.airport_noise_dnl
        WHEN pcl.year > '2020' THEN omp.airport_noise_dnl
        ELSE 52.5
    END AS env_airport_noise_dnl,
    CASE
        WHEN pcl.year <= '2020' THEN an.year
        ELSE 'omp'
    END AS env_airport_noise_data_year,
    pcl.year
FROM {{ source('spatial', 'parcel') }} AS pcl
LEFT JOIN flood_fema
    ON pcl.x_3435 = flood_fema.x_3435
    AND pcl.y_3435 = flood_fema.y_3435
    AND pcl.year = flood_fema.pin_year
LEFT JOIN {{ source('other', 'flood_first_street') }} AS flood_first_street
    ON pcl.pin10 = flood_first_street.pin10
    AND pcl.year >= flood_first_street.year
LEFT JOIN
    (
        SELECT *
        FROM {{ source('other', 'airport_noise') }}
        WHERE year != 'omp'
    )
        AS an
    ON pcl.pin10 = an.pin10
    AND pcl.year = an.year
LEFT JOIN
    (
        SELECT *
        FROM {{ source('other', 'airport_noise') }}
        WHERE year = 'omp'
    )
        AS omp
    ON pcl.pin10 = omp.pin10
    AND pcl.year >= '2021'
LEFT JOIN ohare_years AS oy
    ON pcl.year = oy.pin_year
LEFT JOIN ohare_noise_contour_0000 AS onc0000
    ON pcl.x_3435 = onc0000.x_3435
    AND pcl.y_3435 = onc0000.y_3435
    AND pcl.year = onc0000.pin_year
LEFT JOIN ohare_noise_contour_2640 AS onc2640
    ON pcl.x_3435 = onc2640.x_3435
    AND pcl.y_3435 = onc2640.y_3435
    AND pcl.year = onc2640.pin_year
WHERE pcl.year >= (SELECT MIN(year) FROM distinct_years_rhs)
