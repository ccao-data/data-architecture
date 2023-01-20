-- CTAS to find the 3 nearest neighbor PINs for every PIN for every year
CREATE TABLE IF NOT EXISTS proximity.dist_pin_to_pin_temp
WITH (
    format='Parquet',
    write_compression = 'SNAPPY',
    external_location='s3://ccao-athena-results-us-east-1/dist_pin_to_pin_temp',
    partitioned_by = ARRAY['year']
) AS (
    WITH pin_locations AS (
        SELECT
            pin10,
            year,
            x_3435, y_3435,
            ST_Point(x_3435, y_3435) AS point
        FROM spatial.parcel
    ),
    most_recent_pins AS (
        -- Parcel centroids may shift very slightly over time in GIS shapefiles.
        -- We want to make sure we only grab the most recent instance of a given
        -- parcel to avoid duplicates caused by these slight shifts.
        SELECT x_3435, y_3435,
        RANK() OVER (PARTITION BY pin10 ORDER BY year DESC) AS r
        FROM spatial.parcel
    ),
    distinct_pins AS (
        SELECT DISTINCT x_3435, y_3435
        FROM most_recent_pins
        WHERE r = 1
    ),
    pin_dists AS (
        SELECT *
        FROM (
            SELECT
                dists.*,
                ROW_NUMBER() OVER(PARTITION BY x_3435, y_3435, year ORDER BY dist) AS row_num
            FROM (
                SELECT
                    p.x_3435,
                    p.y_3435,
                    o.year,
                    o.pin10,
                    ST_Distance(ST_Point(p.x_3435, p.y_3435), o.point) AS dist
                FROM distinct_pins p
                INNER JOIN pin_locations o
                    ON ST_Contains(ST_Buffer(ST_Point(p.x_3435, p.y_3435), 1000), o.point)
            ) dists
        )
        WHERE row_num <= 4
    )
    SELECT *
    FROM (
        SELECT
            p.pin10,
            MAX(CASE
                WHEN pd.row_num = 2 THEN pd.pin10
                ELSE NULL
            END) AS nearest_neighbor_1_pin10,
            MAX(CASE
                WHEN pd.row_num = 2 THEN pd.dist
                ELSE NULL
            END) AS nearest_neighbor_1_dist_ft,
            MAX(CASE
                WHEN pd.row_num = 3 THEN pd.pin10
                ELSE NULL
            END) AS nearest_neighbor_2_pin10,
            MAX(CASE
                WHEN pd.row_num = 3 THEN pd.dist
                ELSE NULL
            END) AS nearest_neighbor_2_dist_ft,
            MAX(CASE
                WHEN pd.row_num = 4 THEN pd.pin10
                ELSE NULL
            END) AS nearest_neighbor_3_pin10,
            MAX(CASE
                WHEN pd.row_num = 4 THEN pd.dist
                ELSE NULL
            END) AS nearest_neighbor_3_dist_ft,
            p.year
        FROM spatial.parcel p
        INNER JOIN pin_dists pd
            ON p.x_3435 = pd.x_3435
            AND p.y_3435 = pd.y_3435
            AND p.year = pd.year
        GROUP BY p.pin10, p.year
    )
    WHERE nearest_neighbor_1_pin10 IS NOT NULL
    AND nearest_neighbor_2_pin10 IS NOT NULL
    AND nearest_neighbor_3_pin10 IS NOT NULL
)


-- Create an additional table to fill any PINs missing from the original CTAs
-- These will only be PINs with very distant neighbors
-- You can manually edit the buffer size here to join successively more data
-- CTAS to find the 3 nearest neighbor PINs for every PIN for every year
CREATE TABLE IF NOT EXISTS proximity.dist_pin_to_pin_temp2
WITH (
    format='Parquet',
    write_compression = 'SNAPPY',
    external_location='s3://ccao-athena-results-us-east-1/dist_pin_to_pin_temp2',
    partitioned_by = ARRAY['year']
) AS (
    WITH pin_locations AS (
        SELECT
            pin10,
            year,
            x_3435, y_3435,
            ST_Point(x_3435, y_3435) AS point
        FROM spatial.parcel
    ),
    distinct_pins AS (
        SELECT DISTINCT p.x_3435, p.y_3435
        FROM pin_locations p
        LEFT JOIN proximity.dist_pin_to_pin_temp d
            ON p.pin10 = d.pin10
            AND p.year = d.year
        WHERE nearest_neighbor_1_pin10 IS NULL
    ),
    pin_dists AS (
        SELECT *
        FROM (
            SELECT
                dists.*,
                ROW_NUMBER() OVER(PARTITION BY x_3435, y_3435, year ORDER BY dist) AS row_num
            FROM (
                SELECT
                    p.x_3435,
                    p.y_3435,
                    o.year,
                    o.pin10,
                    ST_Distance(ST_Point(p.x_3435, p.y_3435), o.point) AS dist
                FROM distinct_pins p
                INNER JOIN pin_locations o
                    ON ST_Contains(ST_Buffer(ST_Point(p.x_3435, p.y_3435), 30000), o.point)
            ) dists
        )
        WHERE row_num <= 4
    )
    SELECT
        t.pin10,
        t.nearest_neighbor_1_pin10,
        t.nearest_neighbor_1_dist_ft,
        t.nearest_neighbor_2_pin10,
        t.nearest_neighbor_2_dist_ft,
        t.nearest_neighbor_3_pin10,
        t.nearest_neighbor_3_dist_ft,
        t.year
    FROM (
        SELECT
            p.pin10,
            MAX(CASE
                WHEN pd.row_num = 2 THEN pd.pin10
                ELSE NULL
            END) AS nearest_neighbor_1_pin10,
            MAX(CASE
                WHEN pd.row_num = 2 THEN pd.dist
                ELSE NULL
            END) AS nearest_neighbor_1_dist_ft,
            MAX(CASE
                WHEN pd.row_num = 3 THEN pd.pin10
                ELSE NULL
            END) AS nearest_neighbor_2_pin10,
            MAX(CASE
                WHEN pd.row_num = 3 THEN pd.dist
                ELSE NULL
            END) AS nearest_neighbor_2_dist_ft,
            MAX(CASE
                WHEN pd.row_num = 4 THEN pd.pin10
                ELSE NULL
            END) AS nearest_neighbor_3_pin10,
            MAX(CASE
                WHEN pd.row_num = 4 THEN pd.dist
                ELSE NULL
            END) AS nearest_neighbor_3_dist_ft,
            p.year
        FROM spatial.parcel p
        INNER JOIN pin_dists pd
            ON p.x_3435 = pd.x_3435
            AND p.y_3435 = pd.y_3435
            AND p.year = pd.year
        GROUP BY p.pin10, p.year
    ) t
    LEFT JOIN proximity.dist_pin_to_pin_temp d
        ON t.pin10 = d.pin10
        AND t.year = d.year
    WHERE d.nearest_neighbor_1_pin10 IS NULL
    AND t.nearest_neighbor_1_pin10 IS NOT NULL
    AND t.nearest_neighbor_2_pin10 IS NOT NULL
    AND t.nearest_neighbor_3_pin10 IS NOT NULL
)

-- Consolidate unbucketed files into single files and delete temp table
CREATE TABLE IF NOT EXISTS proximity.dist_pin_to_pin
WITH (
    format='Parquet',
    write_compression = 'SNAPPY',
    external_location='s3://ccao-athena-ctas-us-east-1/proximity/dist_pin_to_pin',
    partitioned_by = ARRAY['year'],
    bucketed_by = ARRAY['pin10'],
    bucket_count = 1
) AS (
    SELECT
        pin10,
        nearest_neighbor_1_pin10,
        nearest_neighbor_1_dist_ft,
        nearest_neighbor_2_pin10,
        nearest_neighbor_2_dist_ft,
        nearest_neighbor_3_pin10,
        nearest_neighbor_3_dist_ft,
        year
    FROM proximity.dist_pin_to_pin_temp
    UNION
    SELECT
        pin10,
        nearest_neighbor_1_pin10,
        nearest_neighbor_1_dist_ft,
        nearest_neighbor_2_pin10,
        nearest_neighbor_2_dist_ft,
        nearest_neighbor_3_pin10,
        nearest_neighbor_3_dist_ft,
        year
    FROM proximity.dist_pin_to_pin_temp2
);

DROP TABLE IF EXISTS proximity.dist_pin_to_pin_temp
DROP TABLE IF EXISTS proximity.dist_pin_to_pin_temp2
