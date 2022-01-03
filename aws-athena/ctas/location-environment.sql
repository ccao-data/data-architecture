CREATE TABLE IF NOT EXISTS location.environment
WITH (
    format='Parquet',
    write_compression = 'SNAPPY',
    external_location='s3://ccao-athena-ctas-us-east-1/location/environment',
    partitioned_by = ARRAY['year'],
    bucketed_by = ARRAY['pin10'],
    bucket_count = 1
) AS (
 WITH distinct_pins AS (
        SELECT DISTINCT x_3435, y_3435
        FROM spatial.parcel
    ),
    distinct_years AS (
        SELECT DISTINCT year
        FROM spatial.parcel
    ),
    flood_fema AS (
        SELECT
            p.x_3435, p.y_3435,
            MAX(cprod.fema_special_flood_hazard_area) AS env_flood_fema_sfha,
            MAX(cprod.fill_year) AS env_flood_fema_data_year,
            cprod.pin_year
        FROM distinct_pins p
        LEFT JOIN (
            SELECT fill_years.*, fill_data.*
            FROM (
                SELECT dy.year AS pin_year, '2021' AS fill_year
                FROM spatial.flood_fema df
                CROSS JOIN distinct_years dy
                WHERE dy.year >= '2021'
                GROUP BY dy.year
            ) fill_years
            CROSS JOIN spatial.flood_fema fill_data
        ) cprod
        ON ST_Within(ST_Point(p.x_3435, p.y_3435), ST_GeomFromBinary(cprod.geometry_3435))
        GROUP BY p.x_3435, p.y_3435, cprod.pin_year
    ),
    ohare_noise_contour_0000 AS (
        SELECT
            p.x_3435, p.y_3435,
            MAX(cprod.airport) AS airport,
            cprod.pin_year
        FROM distinct_pins p
        LEFT JOIN (
            SELECT fill_years.*, fill_data.*
            FROM (
                SELECT dy.year AS pin_year, '2020' AS fill_year
                FROM spatial.ohare_noise_contour df
                CROSS JOIN distinct_years dy
                WHERE dy.year >= '2020'
                GROUP BY dy.year
            ) fill_years
            CROSS JOIN spatial.ohare_noise_contour fill_data
        ) cprod
        ON ST_Within(
            ST_Point(p.x_3435, p.y_3435),
            ST_GeomFromBinary(cprod.geometry_3435)
        )
        GROUP BY p.x_3435, p.y_3435, cprod.pin_year
    ),
    ohare_noise_contour_2640 AS (
        SELECT
            p.x_3435, p.y_3435,
            MAX(cprod.airport) AS airport,
            cprod.pin_year
        FROM distinct_pins p
        LEFT JOIN (
            SELECT fill_years.*, fill_data.*
            FROM (
                SELECT dy.year AS pin_year, '2020' AS fill_year
                FROM spatial.ohare_noise_contour df
                CROSS JOIN distinct_years dy
                WHERE dy.year >= '2020'
                GROUP BY dy.year
            ) fill_years
            CROSS JOIN spatial.ohare_noise_contour fill_data
        ) cprod
        ON ST_Within(
            ST_Point(p.x_3435, p.y_3435),
            ST_Buffer(ST_GeomFromBinary(cprod.geometry_3435), 2640)
        )
        GROUP BY p.x_3435, p.y_3435, cprod.pin_year
    )
    SELECT
        p.pin10,
        env_flood_fema_sfha,
        env_flood_fema_data_year,
        flood_first_street.fs_flood_factor AS env_flood_fs_factor,
        flood_first_street.fs_flood_risk_direction AS env_flood_fs_risk_direction,
        flood_first_street.year AS env_flood_fs_data_year,
        CASE
            WHEN p.year >= '2020' AND ohare_noise_contour_0000.airport IS NOT NULL THEN true
            WHEN p.year >= '2020' AND ohare_noise_contour_0000.airport IS NULL THEN false
            ELSE NULL END AS env_ohare_noise_contour_no_buffer_bool,
        CASE
            WHEN p.year >= '2020' AND ohare_noise_contour_2640.airport IS NOT NULL THEN true
            WHEN p.year >= '2020' AND ohare_noise_contour_2640.airport IS NULL THEN false
            ELSE NULL END AS env_ohare_noise_contour_half_mile_buffer_bool,
        CASE
            WHEN p.year >= '2020' THEN '2020'
            ELSE NULL END AS env_ohare_noise_contour_data_year,
        p.year
    FROM spatial.parcel p
    LEFT JOIN flood_fema
        ON p.x_3435 = flood_fema.x_3435
        AND p.y_3435 = flood_fema.y_3435
        AND p.year = flood_fema.pin_year
    LEFT JOIN other.flood_first_street
        ON p.pin10 = flood_first_street.pin10
        AND p.year >= flood_first_street.year
    LEFT JOIN ohare_noise_contour_0000
        ON p.x_3435 = ohare_noise_contour_0000.x_3435
        AND p.y_3435 = ohare_noise_contour_0000.y_3435
        AND p.year = ohare_noise_contour_0000.pin_year
    LEFT JOIN ohare_noise_contour_2640
        ON p.x_3435 = ohare_noise_contour_2640.x_3435
        AND p.y_3435 = ohare_noise_contour_2640.y_3435
        AND p.year = ohare_noise_contour_2640.pin_year
)