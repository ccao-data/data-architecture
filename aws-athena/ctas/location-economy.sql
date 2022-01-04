CREATE TABLE IF NOT EXISTS location.economy
WITH (
    format='Parquet',
    write_compression = 'SNAPPY',
    external_location='s3://ccao-athena-ctas-us-east-1/location/economy',
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
    distinct_years_rhs AS (
        SELECT DISTINCT year FROM spatial.coordinated_care
        UNION ALL
        SELECT DISTINCT year FROM spatial.enterprise_zone
        UNION ALL
        SELECT DISTINCT year FROM spatial.industrial_growth_zone
        UNION ALL
        SELECT DISTINCT year FROM spatial.qualified_opportunity_zone
    ),
    coordinated_care AS (
        SELECT
            p.x_3435, p.y_3435,
            MAX(cprod.cc_num) AS econ_coordinated_care_area_num,
            MAX(cprod.year) AS econ_coordinated_care_area_data_year,
            cprod.pin_year
        FROM distinct_pins p
        LEFT JOIN (
            SELECT fill_years.pin_year, fill_data.*
            FROM (
                SELECT dy.year AS pin_year, MAX(df.year) AS fill_year
                FROM spatial.coordinated_care df
                CROSS JOIN distinct_years dy
                WHERE dy.year >= df.year
                GROUP BY dy.year
            ) fill_years
            LEFT JOIN spatial.coordinated_care fill_data
                ON fill_years.fill_year = fill_data.year
        ) cprod
        ON ST_Within(ST_Point(p.x_3435, p.y_3435), ST_GeomFromBinary(cprod.geometry_3435))
        GROUP BY p.x_3435, p.y_3435, cprod.pin_year
    ),
    enterprise_zone AS (
        SELECT
            p.x_3435, p.y_3435,
            MAX(cprod.ez_num) AS econ_enterprise_zone_num,
            MAX(cprod.year) AS econ_enterprise_zone_data_year,
            cprod.pin_year
        FROM distinct_pins p
        LEFT JOIN (
            SELECT fill_years.pin_year, fill_data.*
            FROM (
                SELECT dy.year AS pin_year, MAX(df.year) AS fill_year
                FROM spatial.enterprise_zone df
                CROSS JOIN distinct_years dy
                WHERE dy.year >= df.year
                GROUP BY dy.year
            ) fill_years
            LEFT JOIN spatial.enterprise_zone fill_data
                ON fill_years.fill_year = fill_data.year
        ) cprod
        ON ST_Within(ST_Point(p.x_3435, p.y_3435), ST_GeomFromBinary(cprod.geometry_3435))
        GROUP BY p.x_3435, p.y_3435, cprod.pin_year
    ),
    industrial_growth_zone AS (
        SELECT
            p.x_3435, p.y_3435,
            MAX(cprod.igz_num) AS econ_industrial_growth_zone_num,
            MAX(cprod.year) AS econ_industrial_growth_zone_data_year,
            cprod.pin_year
        FROM distinct_pins p
        LEFT JOIN (
            SELECT fill_years.pin_year, fill_data.*
            FROM (
                SELECT dy.year AS pin_year, MAX(df.year) AS fill_year
                FROM spatial.industrial_growth_zone df
                CROSS JOIN distinct_years dy
                WHERE dy.year >= df.year
                GROUP BY dy.year
            ) fill_years
            LEFT JOIN spatial.industrial_growth_zone fill_data
                ON fill_years.fill_year = fill_data.year
        ) cprod
        ON ST_Within(ST_Point(p.x_3435, p.y_3435), ST_GeomFromBinary(cprod.geometry_3435))
        GROUP BY p.x_3435, p.y_3435, cprod.pin_year
    ),
    qualified_opportunity_zone AS (
        SELECT
            p.x_3435, p.y_3435,
            MAX(cprod.geoid) AS econ_qualified_opportunity_zone_num,
            MAX(cprod.year) AS econ_qualified_opportunity_zone_data_year,
            cprod.pin_year
        FROM distinct_pins p
        LEFT JOIN (
            SELECT fill_years.pin_year, fill_data.*
            FROM (
                SELECT dy.year AS pin_year, MAX(df.year) AS fill_year
                FROM spatial.qualified_opportunity_zone df
                CROSS JOIN distinct_years dy
                WHERE dy.year >= df.year
                GROUP BY dy.year
            ) fill_years
            LEFT JOIN spatial.qualified_opportunity_zone fill_data
                ON fill_years.fill_year = fill_data.year
        ) cprod
        ON ST_Within(ST_Point(p.x_3435, p.y_3435), ST_GeomFromBinary(cprod.geometry_3435))
        GROUP BY p.x_3435, p.y_3435, cprod.pin_year
    )
    SELECT
        p.pin10,
        econ_coordinated_care_area_num,
        econ_coordinated_care_area_data_year,
        econ_enterprise_zone_num,
        econ_enterprise_zone_data_year,
        econ_industrial_growth_zone_num,
        econ_industrial_growth_zone_data_year,
        econ_qualified_opportunity_zone_num,
        econ_qualified_opportunity_zone_data_year,
        p.year
    FROM spatial.parcel p
    LEFT JOIN coordinated_care
        ON p.x_3435 = coordinated_care.x_3435
        AND p.y_3435 = coordinated_care.y_3435
        AND p.year = coordinated_care.pin_year
    LEFT JOIN enterprise_zone
        ON p.x_3435 = enterprise_zone.x_3435
        AND p.y_3435 = enterprise_zone.y_3435
        AND p.year = enterprise_zone.pin_year
    LEFT JOIN industrial_growth_zone
        ON p.x_3435 = industrial_growth_zone.x_3435
        AND p.y_3435 = industrial_growth_zone.y_3435
        AND p.year = industrial_growth_zone.pin_year
    LEFT JOIN qualified_opportunity_zone
        ON p.x_3435 = qualified_opportunity_zone.x_3435
        AND p.y_3435 = qualified_opportunity_zone.y_3435
        AND p.year = qualified_opportunity_zone.pin_year
    WHERE p.year >= (SELECT MIN(year) FROM distinct_years_rhs)
)