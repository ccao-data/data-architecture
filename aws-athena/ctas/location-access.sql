CREATE TABLE IF NOT EXISTS location.access
WITH (
    format='Parquet',
    write_compression = 'SNAPPY',
    external_location='s3://ccao-athena-ctas-us-east-1/location/access',
    partitioned_by = ARRAY['year'],
    bucketed_by = ARRAY['pin10'],
    bucket_count = 1
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
        CAST(CAST(MAX(walk.walk_num) AS bigint) AS varchar) AS access_cmap_walk_id,
        MAX(walk.nta_score) AS access_cmap_walk_nta_score,
        MAX(walk.total_score) AS access_cmap_walk_total_score,
        MAX(walk.year) AS access_cmap_walk_data_year,
        p.year
    FROM pin_locations p
    LEFT JOIN spatial.walkability walk
        ON ST_Within(p.centroid, ST_GeomFromBinary(walk.geometry_3435))
    GROUP BY p.pin10, p.year
)