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
        CASE
            WHEN MAX(flood_fema.fema_special_flood_hazard_area) IS NOT NULL THEN '2021'
            ELSE NULL END AS env_flood_fema_data_year,
        MAX(flood_fs.fs_flood_factor) AS env_flood_fs_factor,
        MAX(flood_fs.fs_flood_risk_direction) AS env_flood_fs_risk_direction,
        CASE
            WHEN MAX(flood_fs.fs_flood_factor) IS NOT NULL THEN '2019'
            ELSE NULL END AS env_flood_fs_data_year,
        MAX(CASE
            WHEN ohare_contour_0000.airport IS NOT NULL THEN true
            ELSE false END) AS env_ohare_noise_contour_no_buffer_bool,
        MAX(CASE
            WHEN ohare_contour_2640.airport IS NOT NULL THEN true
            ELSE false END) AS env_ohare_noise_contour_half_mile_buffer_bool,
        '2016' AS env_ohare_noise_contour_data_year,
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