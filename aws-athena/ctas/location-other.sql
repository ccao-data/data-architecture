CREATE TABLE IF NOT EXISTS location.other
WITH (
    format='Parquet',
    write_compression = 'SNAPPY',
    external_location='s3://ccao-athena-ctas-us-east-1/location/other',
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
        MAX(subdivision.pagesubref) AS misc_subdivision_id,
        MAX(CASE
            WHEN uninc_area.agency_desc IS NOT NULL THEN true
            ELSE false END) AS misc_unincorporated_area,
        p.year
    FROM pin_locations p
    LEFT JOIN spatial.subdivision subdivision
        ON ST_Within(p.centroid, ST_GeomFromBinary(subdivision.geometry_3435))
    LEFT JOIN spatial.unincorporated_area uninc_area
        ON ST_Within(p.centroid, ST_GeomFromBinary(uninc_area.geometry_3435))
    GROUP BY p.pin10, p.year
)