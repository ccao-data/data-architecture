CREATE TABLE IF NOT EXISTS location.tax
WITH (
    format='Parquet',
    write_compression = 'SNAPPY',
    external_location='s3://ccao-athena-ctas-us-east-1/location/tax',
    partitioned_by = ARRAY['year']
) AS (
    WITH distinct_pins AS (
        SELECT DISTINCT x_3435, y_3435
        FROM spatial.parcel
        WHERE year >= '2012'
    ),
    distinct_years AS (
        SELECT DISTINCT year
        FROM spatial.parcel
        WHERE year >= '2012'
    ),
    community_college_district AS (
        SELECT
            p.x_3435, p.y_3435,
            MAX(cprod.community_college_district_num) AS tax_community_college_district_num,
            MAX(cprod.community_college_district_name) AS tax_community_college_district_name,
            MAX(cprod.year) AS tax_community_college_district_data_year,
            cprod.pin_year
        FROM distinct_pins p
        LEFT JOIN (
            SELECT fill_years.pin_year, fill_data.*
            FROM (
                SELECT dy.year AS pin_year, MAX(df.year) AS fill_year
                FROM spatial.community_college_district df
                CROSS JOIN distinct_years dy
                WHERE dy.year >= df.year
                GROUP BY dy.year
            ) fill_years
            LEFT JOIN spatial.community_college_district fill_data
                ON fill_years.fill_year = fill_data.year
        ) cprod
        ON ST_Within(ST_Point(p.x_3435, p.y_3435), ST_GeomFromBinary(cprod.geometry_3435))
        GROUP BY p.x_3435, p.y_3435, cprod.pin_year
    ),
    fire_protection_district AS (
        SELECT
            p.x_3435, p.y_3435,
            MAX(cprod.fire_protection_district_num) AS tax_fire_protection_district_num,
            MAX(cprod.fire_protection_district_name) AS tax_fire_protection_district_name,
            MAX(cprod.year) AS tax_fire_protection_district_data_year,
            cprod.pin_year
        FROM distinct_pins p
        LEFT JOIN (
            SELECT fill_years.pin_year, fill_data.*
            FROM (
                SELECT dy.year AS pin_year, MAX(df.year) AS fill_year
                FROM spatial.fire_protection_district df
                CROSS JOIN distinct_years dy
                WHERE dy.year >= df.year
                GROUP BY dy.year
            ) fill_years
            LEFT JOIN spatial.fire_protection_district fill_data
                ON fill_years.fill_year = fill_data.year
        ) cprod
        ON ST_Within(ST_Point(p.x_3435, p.y_3435), ST_GeomFromBinary(cprod.geometry_3435))
        GROUP BY p.x_3435, p.y_3435, cprod.pin_year
    ),
    library_district AS (
        SELECT
            p.x_3435, p.y_3435,
            MAX(cprod.library_district_num) AS tax_library_district_num,
            MAX(cprod.library_district_name) AS tax_library_district_name,
            MAX(cprod.year) AS tax_library_district_data_year,
            cprod.pin_year
        FROM distinct_pins p
        LEFT JOIN (
            SELECT fill_years.pin_year, fill_data.*
            FROM (
                SELECT dy.year AS pin_year, MAX(df.year) AS fill_year
                FROM spatial.library_district df
                CROSS JOIN distinct_years dy
                WHERE dy.year >= df.year
                GROUP BY dy.year
            ) fill_years
            LEFT JOIN spatial.library_district fill_data
                ON fill_years.fill_year = fill_data.year
        ) cprod
        ON ST_Within(ST_Point(p.x_3435, p.y_3435), ST_GeomFromBinary(cprod.geometry_3435))
        GROUP BY p.x_3435, p.y_3435, cprod.pin_year
    ),
    park_district AS (
        SELECT
            p.x_3435, p.y_3435,
            MAX(cprod.park_district_num) AS tax_park_district_num,
            MAX(cprod.park_district_name) AS tax_park_district_name,
            MAX(cprod.year) AS tax_park_district_data_year,
            cprod.pin_year
        FROM distinct_pins p
        LEFT JOIN (
            SELECT fill_years.pin_year, fill_data.*
            FROM (
                SELECT dy.year AS pin_year, MAX(df.year) AS fill_year
                FROM spatial.park_district df
                CROSS JOIN distinct_years dy
                WHERE dy.year >= df.year
                GROUP BY dy.year
            ) fill_years
            LEFT JOIN spatial.park_district fill_data
                ON fill_years.fill_year = fill_data.year
        ) cprod
        ON ST_Within(ST_Point(p.x_3435, p.y_3435), ST_GeomFromBinary(cprod.geometry_3435))
        GROUP BY p.x_3435, p.y_3435, cprod.pin_year
    ),
    sanitation_district AS (
        SELECT
            p.x_3435, p.y_3435,
            MAX(cprod.sanitation_district_num) AS tax_sanitation_district_num,
            MAX(cprod.sanitation_district_name) AS tax_sanitation_district_name,
            MAX(cprod.year) AS tax_sanitation_district_data_year,
            cprod.pin_year
        FROM distinct_pins p
        LEFT JOIN (
            SELECT fill_years.pin_year, fill_data.*
            FROM (
                SELECT dy.year AS pin_year, MAX(df.year) AS fill_year
                FROM spatial.sanitation_district df
                CROSS JOIN distinct_years dy
                WHERE dy.year >= df.year
                GROUP BY dy.year
            ) fill_years
            LEFT JOIN spatial.sanitation_district fill_data
                ON fill_years.fill_year = fill_data.year
        ) cprod
        ON ST_Within(ST_Point(p.x_3435, p.y_3435), ST_GeomFromBinary(cprod.geometry_3435))
        GROUP BY p.x_3435, p.y_3435, cprod.pin_year
    ),
    special_service_area AS (
        SELECT
            p.x_3435, p.y_3435,
            MAX(cprod.special_service_area_num) AS tax_special_service_area_num,
            MAX(cprod.special_service_area_name) AS tax_special_service_area_name,
            MAX(cprod.year) AS tax_special_service_area_data_year,
            cprod.pin_year
        FROM distinct_pins p
        LEFT JOIN (
            SELECT fill_years.pin_year, fill_data.*
            FROM (
                SELECT dy.year AS pin_year, MAX(df.year) AS fill_year
                FROM spatial.special_service_area df
                CROSS JOIN distinct_years dy
                WHERE dy.year >= df.year
                GROUP BY dy.year
            ) fill_years
            LEFT JOIN spatial.special_service_area fill_data
                ON fill_years.fill_year = fill_data.year
        ) cprod
        ON ST_Within(ST_Point(p.x_3435, p.y_3435), ST_GeomFromBinary(cprod.geometry_3435))
        GROUP BY p.x_3435, p.y_3435, cprod.pin_year
    ),
    tif_district AS (
        SELECT
            p.x_3435, p.y_3435,
            MAX(cprod.tif_district_num) AS tax_tif_district_num,
            MAX(cprod.tif_district_name) AS tax_tif_district_name,
            MAX(cprod.year) AS tax_tif_district_data_year,
            cprod.pin_year
        FROM distinct_pins p
        LEFT JOIN (
            SELECT fill_years.pin_year, fill_data.*
            FROM (
                SELECT dy.year AS pin_year, MAX(df.year) AS fill_year
                FROM spatial.tif_district df
                CROSS JOIN distinct_years dy
                WHERE dy.year >= df.year
                GROUP BY dy.year
            ) fill_years
            LEFT JOIN spatial.tif_district fill_data
                ON fill_years.fill_year = fill_data.year
        ) cprod
        ON ST_Within(ST_Point(p.x_3435, p.y_3435), ST_GeomFromBinary(cprod.geometry_3435))
        GROUP BY p.x_3435, p.y_3435, cprod.pin_year
    )
    SELECT
        p.pin10,
        tax_community_college_district_num,
        tax_community_college_district_name,
        tax_community_college_district_data_year,
        tax_fire_protection_district_num,
        tax_fire_protection_district_name,
        tax_fire_protection_district_data_year,
        tax_library_district_num,
        tax_library_district_name,
        tax_library_district_data_year,
        tax_park_district_num,
        tax_park_district_name,
        tax_park_district_data_year,
        tax_sanitation_district_num,
        tax_sanitation_district_name,
        tax_sanitation_district_data_year,
        tax_special_service_area_num,
        tax_special_service_area_name,
        tax_special_service_area_data_year,
        tax_tif_district_num,
        tax_tif_district_name,
        tax_tif_district_data_year,
        p.year
    FROM spatial.parcel p
    LEFT JOIN community_college_district
        ON p.x_3435 = community_college_district.x_3435
        AND p.y_3435 = community_college_district.y_3435
        AND p.year = community_college_district.pin_year
    LEFT JOIN fire_protection_district
        ON p.x_3435 = fire_protection_district.x_3435
        AND p.y_3435 = fire_protection_district.y_3435
        AND p.year = fire_protection_district.pin_year
    LEFT JOIN library_district
        ON p.x_3435 = library_district.x_3435
        AND p.y_3435 = library_district.y_3435
        AND p.year = library_district.pin_year
    LEFT JOIN park_district
        ON p.x_3435 = park_district.x_3435
        AND p.y_3435 = park_district.y_3435
        AND p.year = park_district.pin_year
    LEFT JOIN sanitation_district
        ON p.x_3435 = sanitation_district.x_3435
        AND p.y_3435 = sanitation_district.y_3435
        AND p.year = sanitation_district.pin_year
    LEFT JOIN special_service_area
        ON p.x_3435 = special_service_area.x_3435
        AND p.y_3435 = special_service_area.y_3435
        AND p.year = special_service_area.pin_year
    LEFT JOIN tif_district
        ON p.x_3435 = tif_district.x_3435
        AND p.y_3435 = tif_district.y_3435
        AND p.year = tif_district.pin_year
    WHERE p.year >= '2012'
)