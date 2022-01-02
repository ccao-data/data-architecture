-- CTAS to create a table of foreclosure counts per PIN. Counts are within 1/2
-- mile and past 5 years of each target PIN
CREATE TABLE IF NOT EXISTS proximity.cnt_pin_num_foreclosure
WITH (
    format='Parquet',
    write_compression = 'SNAPPY',
    external_location='s3://ccao-athena-ctas-us-east-1/proximity/cnt_pin_num_foreclosure',
    partitioned_by = ARRAY['year']
) AS (
    WITH pin_locations AS (
        SELECT
            pin10,
            year,
            x_3435, y_3435,
            ST_Point(x_3435, y_3435) AS point
        FROM spatial.parcel
        WHERE year >= '2012'
    ),
    foreclosure_locations AS (
        SELECT *, ST_Buffer(ST_GeomFromBinary(geometry_3435), 2640) AS buffer
        FROM other.foreclosure
    ),
    pins_in_buffers AS (
        SELECT
            p.pin10,
            p.year,
            COUNT(*) AS num_foreclosures_in_half_mile_past_5_years
        FROM pin_locations p
        INNER JOIN foreclosure_locations o
            ON YEAR(o.foreclosure_recording_date)
                BETWEEN CAST(p.year AS int) - 5 AND CAST(p.year AS int)
            AND ST_Contains(o.buffer, p.point)
        GROUP BY p.pin10, p.year
    ),
    pin_counts_in_half_mile AS (
        SELECT
            p.pin10,
            p.year,
            COUNT(*) AS num_pins_in_half_mile
        FROM pin_locations p
        INNER JOIN pin_locations o
            ON p.year = o.year
            AND ST_Distance(p.point, o.point) <= 2640
        GROUP BY p.pin10, p.year
    )
    SELECT
        p.pin10,
        CASE
            WHEN f.num_foreclosures_in_half_mile_past_5_years IS NULL THEN 0
            ELSE f.num_foreclosures_in_half_mile_past_5_years END AS num_foreclosures_in_half_mile_past_5_years,
        CASE
            WHEN c.num_pins_in_half_mile IS NULL THEN 1
            ELSE c.num_pins_in_half_mile END AS num_pins_in_half_mile,
        ROUND(
            CAST(num_foreclosures_in_half_mile_past_5_years AS double) / (
            CAST(num_pins_in_half_mile AS double) / 1000), 2
        ) AS num_fc_per_1000_props_past_5_years,
        p.year
    FROM pin_locations p
    LEFT JOIN pins_in_buffers f
        ON p.pin10 = f.pin10
        AND p.year = f.year
    LEFT JOIN pin_counts_in_half_mile c
        ON p.year = c.year
        AND p.pin10 = c.pin10
)