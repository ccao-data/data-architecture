-- CTAS to create a table counting the number of bus stops within a half mile
-- of each PIN
CREATE TABLE IF NOT EXISTS proximity.cnt_pin_num_bus_stop
WITH (
    FORMAT = 'Parquet',
    WRITE_COMPRESSION = 'SNAPPY',
    EXTERNAL_LOCATION
    = 's3://ccao-athena-ctas-us-east-1/proximity/cnt_pin_num_bus_stop',
    PARTITIONED_BY = ARRAY['year'],
    BUCKETED_BY = ARRAY['pin10'],
    BUCKET_COUNT = 1
) AS (
    WITH distinct_pins AS (
        SELECT DISTINCT
            x_3435,
            y_3435
        FROM spatial.parcel
    ),

    distinct_years_rhs AS (
        SELECT DISTINCT year
        FROM spatial.transit_stop
        WHERE route_type = 3
    ),

    stop_locations AS (
        SELECT *
        FROM spatial.transit_stop
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
        GROUP BY dp.x_3435, dp.y_3435, dp.year
    )

    SELECT
        pcl.pin10,
        COALESCE(xy.num_bus_stop_in_half_mile, 0) AS num_bus_stop_in_half_mile,
        xy.year AS num_bus_stop_data_year,
        pcl.year
    FROM spatial.parcel AS pcl
    LEFT JOIN xy_stop_counts AS xy
        ON pcl.x_3435 = xy.x_3435
        AND pcl.y_3435 = xy.y_3435
        AND pcl.year = xy.year
    WHERE pcl.year >= (SELECT MIN(year) FROM distinct_years_rhs)
)
