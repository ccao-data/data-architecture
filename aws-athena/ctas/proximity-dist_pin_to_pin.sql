-- CTAS to find the 3 nearest neighbor PINs for every PIN for every year
{{
    config(
        materialized='table',
        partitioned_by=['year'],
        bucketed_by=['pin10'],
        bucket_count=1
    )
}}

WITH dist_pin_to_pin AS (
    WITH pin_locations AS (
        SELECT
            pin10,
            year,
            x_3435,
            y_3435,
            ST_POINT(x_3435, y_3435) AS point
        FROM {{ source('spatial', 'parcel') }}
    ),

    most_recent_pins AS (
        -- Parcel centroids may shift very slightly over time in GIS shapefiles.
        -- We want to make sure we only grab the most recent instance of a given
        -- parcel to avoid duplicates caused by these slight shifts.
        SELECT
            x_3435,
            y_3435,
            RANK() OVER (PARTITION BY pin10 ORDER BY year DESC) AS r
        FROM {{ source('spatial', 'parcel') }}
    ),

    distinct_pins AS (
        SELECT DISTINCT
            x_3435,
            y_3435
        FROM most_recent_pins
        WHERE r = 1
    ),

    pin_dists AS (
        SELECT *
        FROM (
            SELECT
                dists.*,
                ROW_NUMBER()
                    OVER (
                        PARTITION BY dists.x_3435, dists.y_3435, dists.year
                        ORDER BY dists.dist
                    )
                    AS row_num
            FROM (
                SELECT
                    dp.x_3435,
                    dp.y_3435,
                    loc.year,
                    loc.pin10,
                    ST_DISTANCE(ST_POINT(dp.x_3435, dp.y_3435), loc.point) AS dist
                FROM distinct_pins AS dp
                INNER JOIN pin_locations AS loc
                    ON ST_CONTAINS(
                        ST_BUFFER(ST_POINT(dp.x_3435, dp.y_3435), 1000), loc.point
                    )
            ) AS dists
        )
        WHERE row_num <= 4
    )

    SELECT *
    FROM (
        SELECT
            pcl.pin10,
            MAX(CASE
                WHEN pd.row_num = 2 THEN pd.pin10
            END) AS nearest_neighbor_1_pin10,
            MAX(CASE
                WHEN pd.row_num = 2 THEN pd.dist
            END) AS nearest_neighbor_1_dist_ft,
            MAX(CASE
                WHEN pd.row_num = 3 THEN pd.pin10
            END) AS nearest_neighbor_2_pin10,
            MAX(CASE
                WHEN pd.row_num = 3 THEN pd.dist
            END) AS nearest_neighbor_2_dist_ft,
            MAX(CASE
                WHEN pd.row_num = 4 THEN pd.pin10
            END) AS nearest_neighbor_3_pin10,
            MAX(CASE
                WHEN pd.row_num = 4 THEN pd.dist
            END) AS nearest_neighbor_3_dist_ft,
            pcl.year
        FROM {{ source('spatial', 'parcel') }} AS pcl
        INNER JOIN pin_dists AS pd
            ON pcl.x_3435 = pd.x_3435
            AND pcl.y_3435 = pd.y_3435
            AND pcl.year = pd.year
        GROUP BY pcl.pin10, pcl.year
    )
    WHERE nearest_neighbor_1_pin10 IS NOT NULL
        AND nearest_neighbor_2_pin10 IS NOT NULL
        AND nearest_neighbor_3_pin10 IS NOT NULL
)

SELECT * FROM dist_pin_to_pin
