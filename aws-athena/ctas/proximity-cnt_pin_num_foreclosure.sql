-- CTAS to create a table of foreclosure counts per PIN. Counts are within 1/2
-- mile and past 5 years of each target PIN
CREATE TABLE IF NOT EXISTS proximity.cnt_pin_num_foreclosure
WITH (
    format='Parquet',
    write_compression = 'SNAPPY',
    external_location='s3://ccao-athena-ctas-us-east-1/proximity/cnt_pin_num_foreclosure',
    partitioned_by = ARRAY['year'],
    bucketed_by = ARRAY['pin10'],
    bucket_count = 1
) AS (
    WITH pin_locations AS (
        SELECT
            pin10,
            year,
            x_3435, y_3435,
            ST_Point(x_3435, y_3435) AS point
        FROM spatial.parcel
    ),
    distinct_pins AS (
        SELECT DISTINCT x_3435, y_3435
        FROM pin_locations
    ),
    foreclosure_locations AS (
        SELECT *, ST_Buffer(ST_GeomFromBinary(geometry_3435), 2640) AS buffer
        FROM other.foreclosure
    ),
    pins_in_buffers AS (
        SELECT
            p.pin10,
            p.year,
            COUNT(*) AS num_foreclosure_in_half_mile_past_5_years
        FROM pin_locations p
        INNER JOIN foreclosure_locations o
            ON YEAR(o.foreclosure_recording_date)
                BETWEEN CAST(p.year AS int) - 5 AND CAST(p.year AS int)
            AND ST_Contains(o.buffer, p.point)
        GROUP BY p.pin10, p.year
    ),
    pin_counts_in_half_mile AS (
        SELECT
            p.x_3435,
            p.y_3435,
            o.year,
            COUNT(*) AS num_pin_in_half_mile
        FROM distinct_pins p
        INNER JOIN pin_locations o
            ON ST_Contains(ST_Buffer(ST_Point(p.x_3435, p.y_3435), 2640), o.point)
        GROUP BY p.x_3435, p.y_3435, o.year
    )
    SELECT
        p.pin10,
        CASE
            WHEN f.num_foreclosure_in_half_mile_past_5_years IS NULL THEN 0
            ELSE f.num_foreclosure_in_half_mile_past_5_years END AS num_foreclosure_in_half_mile_past_5_years,
        CASE
            WHEN c.num_pin_in_half_mile IS NULL THEN 1
            ELSE c.num_pin_in_half_mile END AS num_pin_in_half_mile,
        ROUND(
            CAST(num_foreclosure_in_half_mile_past_5_years AS double) / (
            CAST(num_pin_in_half_mile AS double) / 1000), 2
        ) AS num_foreclosure_per_1000_pin_past_5_years,
        CONCAT(
            CAST(CAST(p.year AS int) - 5 AS varchar),
            ' - ',
            CAST(p.year AS varchar)
        ) AS num_foreclosure_data_year,
        p.year
    FROM pin_locations p
    LEFT JOIN pins_in_buffers f
        ON p.pin10 = f.pin10
        AND p.year = f.year
    LEFT JOIN pin_counts_in_half_mile c
        ON p.x_3435 = c.x_3435
        AND p.y_3435 = c.y_3435
        AND p.year = c.year
)