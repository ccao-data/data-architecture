CREATE TABLE IF NOT EXISTS location.other
WITH (
    format='Parquet',
    write_compression = 'SNAPPY',
    external_location='s3://ccao-athena-ctas-us-east-1/location/other',
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
    subdivision AS (
        SELECT
            p.x_3435, p.y_3435,
            MAX(cprod.pagesubref) AS misc_subdivision_id,
            MAX(cprod.fill_year) AS misc_subdivision_data_year,
            cprod.pin_year
        FROM distinct_pins p
        LEFT JOIN (
            SELECT fill_years.*, fill_data.*
            FROM (
                SELECT dy.year AS pin_year, '2021' AS fill_year
                FROM spatial.subdivision df
                CROSS JOIN distinct_years dy
                WHERE dy.year >= '2021'
                GROUP BY dy.year
            ) fill_years
            CROSS JOIN spatial.subdivision fill_data
        ) cprod
        ON ST_Within(ST_Point(p.x_3435, p.y_3435), ST_GeomFromBinary(cprod.geometry_3435))
        GROUP BY p.x_3435, p.y_3435, cprod.pin_year
    ),
    unincorporated_area AS (
        SELECT
            p.x_3435, p.y_3435,
            MAX(cprod.agency_desc) AS agency_desc,
            cprod.pin_year
        FROM distinct_pins p
        LEFT JOIN (
            SELECT fill_years.*, fill_data.*
            FROM (
                SELECT dy.year AS pin_year, '2014' AS fill_year
                FROM spatial.unincorporated_area df
                CROSS JOIN distinct_years dy
                WHERE dy.year >= '2014'
                GROUP BY dy.year
            ) fill_years
            CROSS JOIN spatial.unincorporated_area fill_data
        ) cprod
        ON ST_Within(ST_Point(p.x_3435, p.y_3435), ST_GeomFromBinary(cprod.geometry_3435))
        GROUP BY p.x_3435, p.y_3435, cprod.pin_year
    )
    SELECT
        p.pin10,
        misc_subdivision_id,
        misc_subdivision_data_year,
        CASE
            WHEN p.year >= '2014' AND unincorporated_area.agency_desc IS NOT NULL THEN true
            WHEN p.year >= '2014' AND unincorporated_area.agency_desc IS NULL THEN false
            ELSE NULL END AS misc_unincorporated_area_bool,
        CASE
            WHEN p.year >= '2014' THEN '2014'
            ELSE NULL END AS misc_unincorporated_area_data_year,
        p.year
    FROM spatial.parcel p
    LEFT JOIN subdivision
        ON p.x_3435 = subdivision.x_3435
        AND p.y_3435 = subdivision.y_3435
        AND p.year = subdivision.pin_year
    LEFT JOIN unincorporated_area
        ON p.x_3435 = unincorporated_area.x_3435
        AND p.y_3435 = unincorporated_area.y_3435
        AND p.year = unincorporated_area.pin_year
)