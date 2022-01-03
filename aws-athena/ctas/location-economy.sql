CREATE TABLE IF NOT EXISTS location.economy
WITH (
    format='Parquet',
    write_compression = 'SNAPPY',
    external_location='s3://ccao-athena-ctas-us-east-1/location/economy',
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
        MAX(econ_coord_care.cc_num) AS econ_consolidated_care_area_num,
        MAX(econ_coord_care.year) AS econ_consolidated_care_area_data_year,
        MAX(econ_ent_zone.ez_num) AS econ_enterprise_zone_num,
        MAX(econ_ent_zone.year) AS econ_enterprise_zone_data_year,
        MAX(econ_igz.igz_num) AS econ_industrial_growth_zone_num,
        MAX(econ_igz.year) AS econ_industrial_growth_zone_data_year,
        MAX(econ_qoz.geoid) AS econ_qualified_opportunity_zone_num,
        MAX(econ_qoz.year) AS econ_qualified_opportunity_zone_data_year,
        p.year
    FROM pin_locations p
    LEFT JOIN spatial.coordinated_care econ_coord_care
        ON p.year >= econ_coord_care.year
        AND ST_Within(p.centroid, ST_GeomFromBinary(econ_coord_care.geometry_3435))
    LEFT JOIN spatial.enterprise_zone econ_ent_zone
        ON p.year >= econ_ent_zone.year
        AND ST_Within(p.centroid, ST_GeomFromBinary(econ_ent_zone.geometry_3435))
    LEFT JOIN spatial.industrial_growth_zone econ_igz
        ON p.year >= econ_igz.year
        AND ST_Within(p.centroid, ST_GeomFromBinary(econ_igz.geometry_3435))
    LEFT JOIN spatial.qualified_opportunity_zone econ_qoz
        ON p.year >= econ_qoz.year
        AND ST_Within(p.centroid, ST_GeomFromBinary(econ_qoz.geometry_3435))
    GROUP BY p.pin10, p.year
)