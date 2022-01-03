CREATE TABLE IF NOT EXISTS location.political
WITH (
    format='Parquet',
    write_compression = 'SNAPPY',
    external_location='s3://ccao-athena-ctas-us-east-1/location/political',
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
        MAX(CAST(CAST(dist_bor.board_of_review_district_num AS integer) AS varchar)) AS cook_board_of_review_district_num,
        MAX(dist_bor.year) AS cook_board_of_review_district_data_year,
        MAX(CAST(CAST(dist_comm.commissioner_district_num AS integer) AS varchar)) AS cook_commissioner_district_num,
        MAX(dist_comm.year) AS cook_commissioner_district_data_year,
        MAX(CAST(CAST(dist_jud.judicial_district_num AS integer) AS varchar)) AS cook_judicial_district_num,
        MAX(dist_jud.year) AS cook_judicial_district_data_year,
        MAX(muni.municipality_num) AS cook_municipality_num,
        MAX(muni.municipality_name) AS cook_municipality_name,
        MAX(muni.year) AS cook_municipality_data_year,
        p.year
    FROM pin_locations p
    LEFT JOIN (SELECT * FROM spatial.board_of_review_district WHERE year = '2012') dist_bor
        ON p.year >= dist_bor.year
        AND ST_Within(p.centroid, ST_GeomFromBinary(dist_bor.geometry_3435))
    LEFT JOIN (SELECT * FROM spatial.commissioner_district WHERE year = '2012') dist_comm
        ON p.year >= dist_comm.year
        AND ST_Within(p.centroid, ST_GeomFromBinary(dist_comm.geometry_3435))
    LEFT JOIN (SELECT * FROM spatial.judicial_district WHERE year = '2012') dist_jud
        ON p.year >= dist_jud.year
        AND ST_Within(p.centroid, ST_GeomFromBinary(dist_jud.geometry_3435))
    LEFT JOIN (SELECT * FROM spatial.municipality WHERE year = '2021') muni
        ON ST_Within(p.centroid, ST_GeomFromBinary(muni.geometry_3435))
    GROUP BY p.pin10, p.year
)