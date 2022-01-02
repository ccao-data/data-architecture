-- View containing each of the PIN-level proximity/distance measurements
-- in the proximity database
CREATE OR REPLACE VIEW default.vw_pin_proximity AS
SELECT
    pin.pin10,
    pin.year,
    bus.num_stops_in_half_mile AS bus_num_stops_in_half_mile,
    sch.num_schools_in_half_mile AS sch_num_schools_in_half_mile,
    sch.avg_rating_in_half_mile AS sch_avg_rating_in_half_mile,
    CAST(CAST(bike.gnis_code AS bigint) AS varchar) AS bike_trail_gnis_code,
    bike.name AS bike_trail_name,
    bike.dist_ft AS bike_trail_dist_ft,
    CAST(CAST(ceme.gnis_code AS bigint) AS varchar) AS cemetery_gnis_code,
    ceme.name AS cemetery_name,
    ceme.dist_ft AS cemetery_dist_ft,
    ctar.route_id AS cta_route_id,
    ctar.route_long_name AS cta_route_name,
    ctar.dist_ft AS cta_route_dist_ft,
    ctas.stop_id AS cta_stop_id,
    ctas.stop_name AS cta_stop_name,
    ctas.dist_ft AS cta_stop_dist_ft,
    CAST(CAST(hosp.gnis_code AS bigint) AS varchar) AS hospital_gnis_code,
    hosp.name AS hospital_name,
    hosp.dist_ft AS hospital_dist_ft,
    lake.dist_ft AS lake_michigan_dist_ft,
    road.osm_id AS road_osm_id,
    road.name AS road_name,
    road.dist_ft AS road_dist_ft,
    metrar.route_id AS metra_route_id,
    metrar.route_long_name AS metra_route_name,
    metrar.dist_ft AS metra_route_dist_ft,
    metras.stop_id AS metra_stop_id,
    metras.stop_name AS metra_stop_name,
    metras.dist_ft AS metra_stop_dist_ft,
    park.name AS park_name,
    park.osm_id AS park_osm_id,
    park.dist_ft AS park_dist_ft,
    rail.name AS railroad_name,
    CAST(CAST(rail.unique_id AS bigint) AS varchar) AS railroad_unique_id,
    rail.dist_ft AS railroad_dist_ft,
    water.name AS water_name,
    water.id AS water_id,
    water.dist_ft AS water_dist_ft
FROM spatial.parcel pin
LEFT JOIN proximity.cnt_pin_num_bus_stop bus
    ON pin.pin10 = bus.pin10
    AND pin.year = bus.year
LEFT JOIN proximity.cnt_pin_num_school sch
    ON pin.pin10 = sch.pin10
    AND pin.year = sch.year
LEFT JOIN proximity.dist_pin_to_bike_trail bike
    ON pin.pin10 = bike.pin10
    AND pin.year = bike.year
LEFT JOIN proximity.dist_pin_to_cemetery ceme
    ON pin.pin10 = ceme.pin10
    AND pin.year = ceme.year
LEFT JOIN proximity.dist_pin_to_cta_route ctar
    ON pin.pin10 = ctar.pin10
    AND pin.year = ctar.year
LEFT JOIN proximity.dist_pin_to_cta_stop ctas
    ON pin.pin10 = ctas.pin10
    AND pin.year = ctas.year
LEFT JOIN proximity.dist_pin_to_hospital hosp
    ON pin.pin10 = hosp.pin10
    AND pin.year = hosp.year
LEFT JOIN proximity.dist_pin_to_lake_michigan lake
    ON pin.pin10 = lake.pin10
    AND pin.year = lake.year
LEFT JOIN proximity.dist_pin_to_major_road road
    ON pin.pin10 = road.pin10
    AND pin.year = road.year
LEFT JOIN proximity.dist_pin_to_metra_route metrar
    ON pin.pin10 = metrar.pin10
    AND pin.year = metrar.year
LEFT JOIN proximity.dist_pin_to_metra_stop metras
    ON pin.pin10 = metras.pin10
    AND pin.year = metras.year
LEFT JOIN proximity.dist_pin_to_park park
    ON pin.pin10 = park.pin10
    AND pin.year = park.year
LEFT JOIN proximity.dist_pin_to_railroad rail
    ON pin.pin10 = rail.pin10
    AND pin.year = rail.year
LEFT JOIN proximity.dist_pin_to_water water
    ON pin.pin10 = water.pin10
    AND pin.year = water.year
WHERE pin.year >= '2013';