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
    SELECT DISTINCT year FROM {{ source('spatial', 'coordinated_care') }}
    UNION ALL
    SELECT DISTINCT year FROM {{ source('spatial', 'enterprise_zone') }}
    UNION ALL
    SELECT DISTINCT year
    FROM {{ source('spatial', 'industrial_growth_zone') }}
    UNION ALL
    SELECT DISTINCT year
    FROM {{ source('spatial', 'qualified_opportunity_zone') }}
    UNION ALL
    SELECT DISTINCT year
    FROM {{ source('spatial', 'central_business_district') }}
),

coordinated_care AS (
    SELECT
        dp.x_3435,
        dp.y_3435,
        MAX(cprod.cc_num) AS econ_coordinated_care_area_num,
        MAX(cprod.year) AS econ_coordinated_care_area_data_year,
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
            FROM {{ source('spatial', 'coordinated_care') }} AS df
            CROSS JOIN distinct_years AS dy
            WHERE dy.year >= df.year
            GROUP BY dy.year
        ) AS fill_years
        LEFT JOIN {{ source('spatial', 'coordinated_care') }} AS fill_data
            ON fill_years.fill_year = fill_data.year
    ) AS cprod
        ON ST_WITHIN(
            ST_POINT(dp.x_3435, dp.y_3435),
            ST_GEOMFROMBINARY(cprod.geometry_3435)
        )
    GROUP BY dp.x_3435, dp.y_3435, cprod.pin_year
),

enterprise_zone AS (
    SELECT
        dp.x_3435,
        dp.y_3435,
        MAX(cprod.ez_num) AS econ_enterprise_zone_num,
        MAX(cprod.year) AS econ_enterprise_zone_data_year,
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
            FROM {{ source('spatial', 'enterprise_zone') }} AS df
            CROSS JOIN distinct_years AS dy
            WHERE dy.year >= df.year
            GROUP BY dy.year
        ) AS fill_years
        LEFT JOIN {{ source('spatial', 'enterprise_zone') }} AS fill_data
            ON fill_years.fill_year = fill_data.year
    ) AS cprod
        ON ST_WITHIN(
            ST_POINT(dp.x_3435, dp.y_3435),
            ST_GEOMFROMBINARY(cprod.geometry_3435)
        )
    GROUP BY dp.x_3435, dp.y_3435, cprod.pin_year
),

industrial_growth_zone AS (
    SELECT
        dp.x_3435,
        dp.y_3435,
        MAX(cprod.igz_num) AS econ_industrial_growth_zone_num,
        MAX(cprod.year) AS econ_industrial_growth_zone_data_year,
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
            FROM {{ source('spatial', 'industrial_growth_zone') }} AS df
            CROSS JOIN distinct_years AS dy
            WHERE dy.year >= df.year
            GROUP BY dy.year
        ) AS fill_years
        LEFT JOIN
            {{ source('spatial', 'industrial_growth_zone') }} AS fill_data
            ON fill_years.fill_year = fill_data.year
    ) AS cprod
        ON ST_WITHIN(
            ST_POINT(dp.x_3435, dp.y_3435),
            ST_GEOMFROMBINARY(cprod.geometry_3435)
        )
    GROUP BY dp.x_3435, dp.y_3435, cprod.pin_year
),

qualified_opportunity_zone AS (
    SELECT
        dp.x_3435,
        dp.y_3435,
        MAX(cprod.geoid) AS econ_qualified_opportunity_zone_num,
        MAX(cprod.year) AS econ_qualified_opportunity_zone_data_year,
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
            FROM {{ source('spatial', 'qualified_opportunity_zone') }} AS df
            CROSS JOIN distinct_years AS dy
            WHERE dy.year >= df.year
            GROUP BY dy.year
        ) AS fill_years
        LEFT JOIN
            {{ source('spatial', 'qualified_opportunity_zone') }}
                AS fill_data
            ON fill_years.fill_year = fill_data.year
    ) AS cprod
        ON ST_WITHIN(
            ST_POINT(dp.x_3435, dp.y_3435),
            ST_GEOMFROMBINARY(cprod.geometry_3435)
        )
    GROUP BY dp.x_3435, dp.y_3435, cprod.pin_year
),

central_business_district AS (
    SELECT
        dp.x_3435,
        dp.y_3435,
        MAX(cprod.cbd_num) AS econ_central_business_district_num,
        MAX(cprod.year) AS econ_central_business_district_data_year,
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
            FROM {{ source('spatial', 'central_business_district') }} AS df
            CROSS JOIN distinct_years AS dy
            WHERE dy.year >= df.year
            GROUP BY dy.year
        ) AS fill_years
        LEFT JOIN
            {{ source('spatial', 'central_business_district') }}
                AS fill_data
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
    cc.econ_coordinated_care_area_num,
    cc.econ_coordinated_care_area_data_year,
    ez.econ_enterprise_zone_num,
    ez.econ_enterprise_zone_data_year,
    igz.econ_industrial_growth_zone_num,
    igz.econ_industrial_growth_zone_data_year,
    qoz.econ_qualified_opportunity_zone_num,
    qoz.econ_qualified_opportunity_zone_data_year,
    cbd.econ_central_business_district_num,
    cbd.econ_central_business_district_data_year,
    pcl.year
FROM {{ source('spatial', 'parcel') }} AS pcl
LEFT JOIN coordinated_care AS cc
    ON pcl.x_3435 = cc.x_3435
    AND pcl.y_3435 = cc.y_3435
    AND pcl.year = cc.pin_year
LEFT JOIN enterprise_zone AS ez
    ON pcl.x_3435 = ez.x_3435
    AND pcl.y_3435 = ez.y_3435
    AND pcl.year = ez.pin_year
LEFT JOIN industrial_growth_zone AS igz
    ON pcl.x_3435 = igz.x_3435
    AND pcl.y_3435 = igz.y_3435
    AND pcl.year = igz.pin_year
LEFT JOIN qualified_opportunity_zone AS qoz
    ON pcl.x_3435 = qoz.x_3435
    AND pcl.y_3435 = qoz.y_3435
    AND pcl.year = qoz.pin_year
LEFT JOIN central_business_district AS cbd
    ON pcl.x_3435 = cbd.x_3435
    AND pcl.y_3435 = cbd.y_3435
    AND pcl.year = cbd.pin_year
WHERE pcl.year >= (SELECT MIN(year) FROM distinct_years_rhs)
