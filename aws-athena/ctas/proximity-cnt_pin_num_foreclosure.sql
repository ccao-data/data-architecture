-- CTAS to create a table of foreclosure counts per PIN. Counts are within 1/2
-- mile and past 5 years of each target PIN
CREATE TABLE IF NOT EXISTS proximity.cnt_pin_num_foreclosure
WITH (
    FORMAT = 'Parquet',
    WRITE_COMPRESSION = 'SNAPPY',
    EXTERNAL_LOCATION
    = 's3://ccao-athena-ctas-us-east-1/proximity/cnt_pin_num_foreclosure',
    PARTITIONED_BY = ARRAY['year'],
    BUCKETED_BY = ARRAY['pin10'],
    BUCKET_COUNT = 1
) AS (
    WITH pin_locations AS (
        SELECT
            pin10,
            year,
            x_3435,
            y_3435,
            ST_POINT(x_3435, y_3435) AS point
        FROM spatial.parcel
    ),

    distinct_pins AS (
        SELECT DISTINCT
            x_3435,
            y_3435
        FROM pin_locations
    ),

    foreclosure_locations AS (
        SELECT
            *,
            ST_BUFFER(ST_GEOMFROMBINARY(geometry_3435), 2640) AS buffer
        FROM sale.foreclosure
    ),

    pins_in_buffers AS (
        SELECT
            p.pin10,
            p.year,
            COUNT(*) AS num_foreclosure_in_half_mile_past_5_years
        FROM pin_locations AS p
        INNER JOIN foreclosure_locations AS o
            ON YEAR(o.foreclosure_recording_date)
            BETWEEN CAST(p.year AS INT) - 5 AND CAST(p.year AS INT)
            AND ST_CONTAINS(o.buffer, p.point)
        GROUP BY p.pin10, p.year
    ),

    pin_counts_in_half_mile AS (
        SELECT
            p.x_3435,
            p.y_3435,
            o.year,
            COUNT(*) AS num_pin_in_half_mile
        FROM distinct_pins AS p
        INNER JOIN pin_locations AS o
            ON ST_CONTAINS(
                ST_BUFFER(ST_POINT(p.x_3435, p.y_3435), 2640), o.point
            )
        GROUP BY p.x_3435, p.y_3435, o.year
    )

    SELECT
        p.pin10,
        COALESCE(
            f.num_foreclosure_in_half_mile_past_5_years,
            0
        ) AS num_foreclosure_in_half_mile_past_5_years,
        COALESCE(c.num_pin_in_half_mile, 1) AS num_pin_in_half_mile,
        ROUND(
            CAST(num_foreclosure_in_half_mile_past_5_years AS DOUBLE) / (
                CAST(num_pin_in_half_mile AS DOUBLE) / 1000
            ), 2
        ) AS num_foreclosure_per_1000_pin_past_5_years,
        CONCAT(
            CAST(CAST(p.year AS INT) - 5 AS VARCHAR),
            ' - ',
            CAST(p.year AS VARCHAR)
        ) AS num_foreclosure_data_year,
        p.year
    FROM pin_locations AS p
    LEFT JOIN pins_in_buffers AS f
        ON p.pin10 = f.pin10
        AND p.year = f.year
    LEFT JOIN pin_counts_in_half_mile AS c
        ON p.x_3435 = c.x_3435
        AND p.y_3435 = c.y_3435
        AND p.year = c.year
)
