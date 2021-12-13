-- View containing each of the PIN-level proximity/distance measurements
-- in the proximity database
CREATE OR REPLACE VIEW default.vw_pin_proximity AS
SELECT 
    pin.pin10,
    pin.year,
    bus.num_stops_in_half_mile AS bus_num_stops_in_half_mile,
    cta.stop_id AS cta_stop_id,
    cta.stop_name AS cta_stop_name,
    cta.dist_ft AS cta_dist_ft,
    metra.stop_id AS metra_stop_id,
    metra.stop_name AS metra_stop_name,
    metra.dist_ft AS metra_dist_ft,
    road.osm_id AS road_osm_id,
    road.name AS road_name,
    road.dist_ft AS road_dist_ft
FROM spatial.parcel pin
LEFT JOIN proximity.cnt_pin_num_bus_stop bus 
    ON pin.pin10 = bus.pin10
    AND pin.year = bus.year
LEFT JOIN proximity.dist_pin_to_cta_stop cta 
    ON pin.pin10 = cta.pin10
    AND pin.year = cta.year
LEFT JOIN proximity.dist_pin_to_metra_stop metra 
    ON pin.pin10 = metra.pin10
    AND pin.year = metra.year
LEFT JOIN proximity.dist_pin_to_major_road road 
    ON pin.pin10 = road.pin10
    AND pin.year = road.year   
WHERE pin.year >= (SELECT MIN(year) FROM spatial.transit_stop);
