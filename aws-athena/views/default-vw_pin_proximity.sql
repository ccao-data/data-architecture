-- View containing each of the PIN-level proximity/distance measurements
-- in the proximity database
CREATE OR REPLACE VIEW default.vw_pin_proximity AS
SELECT
    pin.pin10,
    pin.year,
    bus.num_stops_in_half_mile AS bus_num_stops_in_half_mile,
    ctas.stop_id AS cta_stop_id,
    ctas.stop_name AS cta_stop_name,
    ctas.dist_ft AS cta_stop_dist_ft,
    ctar.route_id AS cta_route_id,
    ctar.route_long_name AS cta_route_name,
    ctar.dist_ft AS cta_route_dist_ft,
    metras.stop_id AS metra_stop_id,
    metras.stop_name AS metra_stop_name,
    metras.dist_ft AS metra_stop_dist_ft,
    metrar.route_id AS metra_route_id,
    metrar.route_long_name AS metra_route_name,
    metrar.dist_ft AS metra_route_dist_ft,
    road.osm_id AS road_osm_id,
    road.name AS road_name,
    road.dist_ft AS road_dist_ft,
    lake.dist_ft AS lake_dist_ft
FROM spatial.parcel pin
LEFT JOIN proximity.cnt_pin_num_bus_stop bus
    ON pin.pin10 = bus.pin10
    AND pin.year = bus.year
LEFT JOIN proximity.dist_pin_to_cta_stop ctas
    ON pin.pin10 = ctas.pin10
    AND pin.year = ctas.year
LEFT JOIN proximity.dist_pin_to_cta_route ctar
    ON pin.pin10 = ctar.pin10
    AND pin.year = ctar.year
LEFT JOIN proximity.dist_pin_to_metra_stop metras
    ON pin.pin10 = metras.pin10
    AND pin.year = metras.year
LEFT JOIN proximity.dist_pin_to_metra_route metrar
    ON pin.pin10 = metrar.pin10
    AND pin.year = metrar.year
LEFT JOIN proximity.dist_pin_to_major_road road
    ON pin.pin10 = road.pin10
    AND pin.year = road.year
LEFT JOIN proximity.dist_pin_to_lake_michigan lake
    ON pin.pin10 = lake.pin10
    AND pin.year = lake.year
WHERE pin.year >= (SELECT MIN(year) FROM spatial.transit_stop);