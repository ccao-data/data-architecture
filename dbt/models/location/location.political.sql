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
    FROM {{ source('spatial', 'board_of_review_district') }}
    UNION ALL
    SELECT DISTINCT year
    FROM {{ source('spatial', 'commissioner_district') }}
    UNION ALL
    SELECT DISTINCT year FROM {{ source('spatial', 'judicial_district') }}
    UNION ALL
    SELECT DISTINCT year FROM {{ source('spatial', 'ward') }}
),

board_of_review_district AS (
    SELECT
        dp.x_3435,
        dp.y_3435,
        MAX(
            CAST(
                CAST(
                    cprod.board_of_review_district_num AS INTEGER
                ) AS VARCHAR
            )
        ) AS cook_board_of_review_district_num,
        MAX(cprod.year) AS cook_board_of_review_district_data_year,
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
            FROM {{ source('spatial', 'board_of_review_district') }} AS df
            CROSS JOIN distinct_years AS dy
            WHERE dy.year >= df.year
            GROUP BY dy.year
        ) AS fill_years
        LEFT JOIN
            {{ source('spatial', 'board_of_review_district') }} AS fill_data
            ON fill_years.fill_year = fill_data.year
    ) AS cprod
        ON ST_WITHIN(
            ST_POINT(dp.x_3435, dp.y_3435),
            ST_GEOMFROMBINARY(cprod.geometry_3435)
        )
    GROUP BY dp.x_3435, dp.y_3435, cprod.pin_year
),

commissioner_district AS (
    SELECT
        dp.x_3435,
        dp.y_3435,
        MAX(
            CAST(
                CAST(cprod.commissioner_district_num AS INTEGER) AS VARCHAR
            )
        ) AS cook_commissioner_district_num,
        MAX(cprod.year) AS cook_commissioner_district_data_year,
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
            FROM {{ source('spatial', 'commissioner_district') }} AS df
            CROSS JOIN distinct_years AS dy
            WHERE dy.year >= df.year
            GROUP BY dy.year
        ) AS fill_years
        LEFT JOIN
            {{ source('spatial', 'commissioner_district') }} AS fill_data
            ON fill_years.fill_year = fill_data.year
    ) AS cprod
        ON ST_WITHIN(
            ST_POINT(dp.x_3435, dp.y_3435),
            ST_GEOMFROMBINARY(cprod.geometry_3435)
        )
    GROUP BY dp.x_3435, dp.y_3435, cprod.pin_year
),

judicial_district AS (
    SELECT
        dp.x_3435,
        dp.y_3435,
        MAX(CAST(CAST(cprod.judicial_district_num AS INTEGER) AS VARCHAR))
            AS cook_judicial_district_num,
        MAX(cprod.year) AS cook_judicial_district_data_year,
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
            FROM {{ source('spatial', 'judicial_district') }} AS df
            CROSS JOIN distinct_years AS dy
            WHERE dy.year >= df.year
            GROUP BY dy.year
        ) AS fill_years
        LEFT JOIN {{ source('spatial', 'judicial_district') }} AS fill_data
            ON fill_years.fill_year = fill_data.year
    ) AS cprod
        ON ST_WITHIN(
            ST_POINT(dp.x_3435, dp.y_3435),
            ST_GEOMFROMBINARY(cprod.geometry_3435)
        )
    GROUP BY dp.x_3435, dp.y_3435, cprod.pin_year
),

ward_chicago AS (
    SELECT
        dp.x_3435,
        dp.y_3435,
        MAX(cprod.ward_num) AS ward_num,
        MAX(cprod.ward_name) AS ward_name,
        MAX(
            CASE
                WHEN
                    SUBSTR(cprod.ward_name, 1, 1) = 'c'
                    THEN cprod.year
            END
        ) AS ward_chicago_data_year,
        cprod.pin_year
    FROM distinct_pins AS dp
    LEFT JOIN (
        SELECT
            fill_years.pin_year,
            fill_data.*
        FROM (
            SELECT
                dy.year AS pin_year,
                MAX(
                    CASE
                        WHEN
                            SUBSTR(df.ward_name, 1, 1) = 'c'
                            THEN df.year
                    END
                ) AS fill_year
            FROM {{ source('spatial', 'ward') }} AS df
            CROSS JOIN distinct_years AS dy
            WHERE dy.year >= df.year
            GROUP BY dy.year
        ) AS fill_years
        LEFT JOIN {{ source('spatial', 'ward') }} AS fill_data
            ON fill_years.fill_year = fill_data.year
    ) AS cprod
        ON ST_WITHIN(
            ST_POINT(dp.x_3435, dp.y_3435),
            ST_GEOMFROMBINARY(cprod.geometry_3435)
        )
    GROUP BY dp.x_3435, dp.y_3435, cprod.pin_year
),

ward_evanston AS (
    SELECT
        dp.x_3435,
        dp.y_3435,
        MAX(cprod.ward_num) AS ward_num,
        MAX(cprod.ward_name) AS ward_name,
        MAX(
            CASE
                WHEN
                    SUBSTR(cprod.ward_name, 1, 1) = 'e'
                    THEN cprod.year
            END
        ) AS ward_evanston_data_year,
        cprod.pin_year
    FROM distinct_pins AS dp
    LEFT JOIN (
        SELECT
            fill_years.pin_year,
            fill_data.*
        FROM (
            SELECT
                dy.year AS pin_year,
                MAX(
                    CASE
                        WHEN
                            SUBSTR(df.ward_name, 1, 1) = 'e'
                            THEN df.year
                    END
                ) AS fill_year
            FROM {{ source('spatial', 'ward') }} AS df
            CROSS JOIN distinct_years AS dy
            WHERE dy.year >= df.year
            GROUP BY dy.year
        ) AS fill_years
        LEFT JOIN {{ source('spatial', 'ward') }} AS fill_data
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
    brd.cook_board_of_review_district_num,
    brd.cook_board_of_review_district_data_year,
    cd.cook_commissioner_district_num,
    cd.cook_commissioner_district_data_year,
    jd.cook_judicial_district_num,
    jd.cook_judicial_district_data_year,
    CAST(COALESCE(we.ward_num, wc.ward_num) AS VARCHAR) AS ward_num,
    COALESCE(we.ward_name, wc.ward_name) AS ward_name,
    wc.ward_chicago_data_year,
    we.ward_evanston_data_year,
    pcl.year
FROM {{ source('spatial', 'parcel') }} AS pcl
LEFT JOIN board_of_review_district AS brd
    ON pcl.x_3435 = brd.x_3435
    AND pcl.y_3435 = brd.y_3435
    AND pcl.year = brd.pin_year
LEFT JOIN commissioner_district AS cd
    ON pcl.x_3435 = cd.x_3435
    AND pcl.y_3435 = cd.y_3435
    AND pcl.year = cd.pin_year
LEFT JOIN judicial_district AS jd
    ON pcl.x_3435 = jd.x_3435
    AND pcl.y_3435 = jd.y_3435
    AND pcl.year = jd.pin_year
LEFT JOIN ward_chicago AS wc
    ON pcl.x_3435 = wc.x_3435
    AND pcl.y_3435 = wc.y_3435
    AND pcl.year = wc.pin_year
LEFT JOIN ward_evanston AS we
    ON pcl.x_3435 = we.x_3435
    AND pcl.y_3435 = we.y_3435
    AND pcl.year = we.pin_year
WHERE pcl.year >= (SELECT MIN(year) FROM distinct_years_rhs)
