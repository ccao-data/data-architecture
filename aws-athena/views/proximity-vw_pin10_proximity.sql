-- View containing each of the PIN-level proximity/distance measurements
-- in the proximity database
CREATE OR REPLACE VIEW proximity.vw_pin10_proximity AS
SELECT
    pin.pin10,
    pin.year,
    num_pin_in_half_mile,
    num_bus_stop_in_half_mile,
    num_bus_stop_data_year,
    num_foreclosure_in_half_mile_past_5_years,
    num_foreclosure_per_1000_pin_past_5_years,
    num_foreclosure_data_year,
    num_school_in_half_mile,
    num_school_with_rating_in_half_mile,
    avg_school_rating_in_half_mile,
    num_school_data_year,
    num_school_rating_data_year,
    nearest_bike_trail_id,
    nearest_bike_trail_name,
    nearest_bike_trail_dist_ft,
    nearest_bike_trail_data_year,
    nearest_cemetery_gnis_code,
    nearest_cemetery_name,
    nearest_cemetery_dist_ft,
    nearest_cemetery_data_year,
    nearest_cta_route_id,
    nearest_cta_route_name,
    nearest_cta_route_dist_ft,
    nearest_cta_route_data_year,
    nearest_cta_stop_id,
    nearest_cta_stop_name,
    nearest_cta_stop_dist_ft,
    nearest_cta_stop_data_year,
    nearest_golf_course_id,
    nearest_golf_course_dist_ft,
    nearest_golf_course_data_year,
    nearest_hospital_gnis_code,
    nearest_hospital_name,
    nearest_hospital_dist_ft,
    nearest_hospital_data_year,
    lake_michigan_dist_ft,
    lake_michigan_data_year,
    nearest_major_road_osm_id,
    nearest_major_road_name,
    nearest_major_road_dist_ft,
    nearest_major_road_data_year,
    nearest_metra_route_id,
    nearest_metra_route_name,
    nearest_metra_route_dist_ft,
    nearest_metra_route_data_year,
    nearest_metra_stop_id,
    nearest_metra_stop_name,
    nearest_metra_stop_dist_ft,
    nearest_metra_stop_data_year,
    nearest_park_osm_id,
    nearest_park_name,
    nearest_park_dist_ft,
    nearest_park_data_year,
    nearest_railroad_id,
    nearest_railroad_name,
    nearest_railroad_dist_ft,
    nearest_railroad_data_year,
    nearest_water_id,
    nearest_water_name,
    nearest_water_dist_ft,
    nearest_water_data_year,
    nearest_neighbor_1_pin10,
    nearest_neighbor_1_dist_ft,
    nearest_neighbor_2_pin10,
    nearest_neighbor_2_dist_ft,
    nearest_neighbor_3_pin10,
    nearest_neighbor_3_dist_ft
FROM spatial.parcel pin
LEFT JOIN proximity.cnt_pin_num_bus_stop
    ON pin.pin10 = cnt_pin_num_bus_stop.pin10
    AND pin.year = cnt_pin_num_bus_stop.year
LEFT JOIN proximity.cnt_pin_num_foreclosure
    ON pin.pin10 = cnt_pin_num_foreclosure.pin10
    AND pin.year = cnt_pin_num_foreclosure.year
LEFT JOIN proximity.cnt_pin_num_school
    ON pin.pin10 = cnt_pin_num_school.pin10
    AND pin.year = cnt_pin_num_school.year
LEFT JOIN proximity.dist_pin_to_bike_trail
    ON pin.pin10 = dist_pin_to_bike_trail.pin10
    AND pin.year = dist_pin_to_bike_trail.year
LEFT JOIN proximity.dist_pin_to_cemetery
    ON pin.pin10 = dist_pin_to_cemetery.pin10
    AND pin.year = dist_pin_to_cemetery.year
LEFT JOIN proximity.dist_pin_to_cta_route
    ON pin.pin10 = dist_pin_to_cta_route.pin10
    AND pin.year = dist_pin_to_cta_route.year
LEFT JOIN proximity.dist_pin_to_cta_stop
    ON pin.pin10 = dist_pin_to_cta_stop.pin10
    AND pin.year = dist_pin_to_cta_stop.year
LEFT JOIN proximity.dist_pin_to_golf_course
    ON pin.pin10 = dist_pin_to_golf_course.pin10
    AND pin.year = dist_pin_to_golf_course.year
LEFT JOIN proximity.dist_pin_to_hospital
    ON pin.pin10 = dist_pin_to_hospital.pin10
    AND pin.year = dist_pin_to_hospital.year
LEFT JOIN proximity.dist_pin_to_lake_michigan
    ON pin.pin10 = dist_pin_to_lake_michigan.pin10
    AND pin.year = dist_pin_to_lake_michigan.year
LEFT JOIN proximity.dist_pin_to_major_road
    ON pin.pin10 = dist_pin_to_major_road.pin10
    AND pin.year = dist_pin_to_major_road.year
LEFT JOIN proximity.dist_pin_to_metra_route
    ON pin.pin10 = dist_pin_to_metra_route.pin10
    AND pin.year = dist_pin_to_metra_route.year
LEFT JOIN proximity.dist_pin_to_metra_stop
    ON pin.pin10 = dist_pin_to_metra_stop.pin10
    AND pin.year = dist_pin_to_metra_stop.year
LEFT JOIN proximity.dist_pin_to_park
    ON pin.pin10 = dist_pin_to_park.pin10
    AND pin.year = dist_pin_to_park.year
LEFT JOIN proximity.dist_pin_to_railroad
    ON pin.pin10 = dist_pin_to_railroad.pin10
    AND pin.year = dist_pin_to_railroad.year
LEFT JOIN proximity.dist_pin_to_water
    ON pin.pin10 = dist_pin_to_water.pin10
    AND pin.year = dist_pin_to_water.year
LEFT JOIN proximity.dist_pin_to_pin
    ON pin.pin10 = dist_pin_to_pin.pin10
    AND pin.year = dist_pin_to_pin.year