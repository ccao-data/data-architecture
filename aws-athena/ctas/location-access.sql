CREATE TABLE IF NOT EXISTS location.access
WITH (
    format='Parquet',
    write_compression = 'SNAPPY',
    external_location='s3://ccao-athena-ctas-us-east-1/location/access',
    partitioned_by = ARRAY['year'],
    bucketed_by = ARRAY['pin10'],
    bucket_count = 1
) AS (
    WITH most_recent_pins AS (
        SELECT x_3435, y_3435,
        RANK() OVER (PARTITION BY pin10 ORDER BY year DESC) AS r
        FROM spatial.parcel
    ),
    distinct_pins AS (
        SELECT DISTINCT x_3435, y_3435
        FROM most_recent_pins
        WHERE r = 1
    ),
    distinct_years AS (
        SELECT DISTINCT year
        FROM spatial.parcel
    ),
    distinct_years_rhs AS (
        SELECT DISTINCT year
        FROM spatial.walkability
    ),
    walkability AS (
        SELECT
            p.x_3435, p.y_3435,
            CAST(CAST(MAX(cprod.walk_num) AS bigint) AS varchar) AS access_cmap_walk_id,
            MAX(cprod.nta_score) AS access_cmap_walk_nta_score,
            MAX(cprod.total_score) AS access_cmap_walk_total_score,
            MAX(cprod.year) AS access_cmap_walk_data_year,
            cprod.pin_year
        FROM distinct_pins p
        LEFT JOIN (
            SELECT fill_years.pin_year, fill_data.*
            FROM (
                SELECT dy.year AS pin_year, MAX(df.year) AS fill_year
                FROM spatial.walkability df
                CROSS JOIN distinct_years dy
                WHERE dy.year >= df.year
                GROUP BY dy.year
            ) fill_years
            LEFT JOIN spatial.walkability fill_data
                ON fill_years.fill_year = fill_data.year
        ) cprod
        ON ST_Within(ST_Point(p.x_3435, p.y_3435), ST_GeomFromBinary(cprod.geometry_3435))
        GROUP BY p.x_3435, p.y_3435, cprod.pin_year
    )
    SELECT
        p.pin10,
        access_cmap_walk_id,
        access_cmap_walk_nta_score,
        access_cmap_walk_total_score,
        access_cmap_walk_data_year,
        p.year
    FROM spatial.parcel p
    LEFT JOIN walkability
        ON p.x_3435 = walkability.x_3435
        AND p.y_3435 = walkability.y_3435
        AND p.year = walkability.pin_year
    WHERE p.year >= (SELECT MIN(year) FROM distinct_years_rhs)
)