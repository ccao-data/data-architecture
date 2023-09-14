-- CTAS to create a table counting the number of bus stops within a half mile
-- of each PIN
{{
    config(
        materialized='table',
        partitioned_by=['year'],
        bucketed_by=['pin10'],
        bucket_count=1
    )
}}

WITH cnt_pin_num_bus_stop AS (
    WITH distinct_pins AS (
        SELECT DISTINCT
            x_3435,
            y_3435
        FROM {{ source('spatial', 'parcel') }}
    ),

    distinct_years_rhs AS (
        SELECT DISTINCT year
        FROM {{ source('spatial', 'transit_stop') }}
        WHERE route_type = 3
    ),

    stop_locations AS (
        SELECT *
        FROM {{ source('spatial', 'transit_stop') }}
        WHERE route_type = 3
    ),

    xy_stop_counts AS (
        SELECT
            dp.x_3435,
            dp.y_3435,
            loc.year,
            COUNT(*) AS num_bus_stop_in_half_mile
        FROM distinct_pins AS dp
        INNER JOIN stop_locations AS loc
            ON ST_CONTAINS(
                ST_BUFFER(ST_GEOMFROMBINARY(loc.geometry_3435), 2640),
                ST_POINT(dp.x_3435, dp.y_3435)
            )
        GROUP BY dp.x_3435, dp.y_3435, loc.year
    )

    SELECT
        pcl.pin10,
        COALESCE(xy.num_bus_stop_in_half_mile, 0) AS num_bus_stop_in_half_mile,
        xy.year AS num_bus_stop_data_year,
        pcl.year
    FROM {{ source('spatial', 'parcel') }} AS pcl
    LEFT JOIN xy_stop_counts AS xy
        ON pcl.x_3435 = xy.x_3435
        AND pcl.y_3435 = xy.y_3435
        AND pcl.year = xy.year
    WHERE pcl.year >= (SELECT MIN(year) FROM distinct_years_rhs)
)

SELECT * FROM cnt_pin_num_bus_stop
