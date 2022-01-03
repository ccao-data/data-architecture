CREATE TABLE IF NOT EXISTS location.environment
WITH (
    format='Parquet',
    write_compression = 'SNAPPY',
    external_location='s3://ccao-athena-ctas-us-east-1/location/environment',
    partitioned_by = ARRAY['year']
) AS (
    WITH pin_locations AS (
        SELECT
            pin10,
            year,
            ST_Point(x_3435, y_3435) AS centroid
        FROM spatial.parcel
        WHERE year >= '2012'
    )
    SELECT
        p.pin10,
        MAX(flood_fema.fema_special_flood_hazard_area) AS env_flood_fema_sfha,
        MAX(flood_fs.fs_flood_factor) AS env_flood_fs_factor,
        MAX(flood_fs.fs_flood_risk_direction) AS env_flood_fs_risk_direction,
        MAX(CASE
            WHEN ohare_contour_0000.airport IS NOT NULL THEN true
            ELSE false END) AS env_ohare_noise_contour_no_buffer,
        MAX(CASE
            WHEN ohare_contour_2640.airport IS NOT NULL THEN true
            ELSE false END) AS env_ohare_noise_contour_half_mil_buffer,
        p.year
    FROM pin_locations p
    LEFT JOIN spatial.flood_fema flood_fema
        ON ST_Within(p.centroid, ST_GeomFromBinary(flood_fema.geometry_3435))
    LEFT JOIN other.flood_first_street flood_fs
        ON p.pin10 = flood_fs.pin10
        AND p.year = flood_fs.year
    LEFT JOIN spatial.ohare_noise_contour ohare_contour_0000
        ON ST_Within(p.centroid, ST_GeomFromBinary(ohare_contour_0000.geometry_3435))
    LEFT JOIN spatial.ohare_noise_contour ohare_contour_2640
        ON ST_Within(p.centroid, ST_Buffer(ST_GeomFromBinary(ohare_contour_2640.geometry_3435), 2640))
    GROUP BY p.pin10, p.year
)