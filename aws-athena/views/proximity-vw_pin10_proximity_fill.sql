-- View containing each of the PIN-level proximity/distance measurements
-- in the proximity database
SELECT
    pin.pin10,
    pin.year,

    cnt_pin_num_foreclosure.num_pin_in_half_mile,
    cnt_pin_num_bus_stop.num_bus_stop_in_half_mile,
    cnt_pin_num_bus_stop.num_bus_stop_data_year,

    cnt_pin_num_foreclosure.num_foreclosure_in_half_mile_past_5_years,
    cnt_pin_num_foreclosure.num_foreclosure_per_1000_pin_past_5_years,
    cnt_pin_num_foreclosure.num_foreclosure_data_year,

    num_school.num_school_in_half_mile,
    num_school_rating.num_school_with_rating_in_half_mile,
    num_school_rating.avg_school_rating_in_half_mile,
    num_school.num_school_data_year,
    num_school_rating.num_school_rating_data_year,

    dist_pin_to_bike_trail.nearest_bike_trail_id,
    dist_pin_to_bike_trail.nearest_bike_trail_name,
    dist_pin_to_bike_trail.nearest_bike_trail_dist_ft,
    dist_pin_to_bike_trail.nearest_bike_trail_data_year,

    dist_pin_to_cemetery.nearest_cemetery_gnis_code,
    dist_pin_to_cemetery.nearest_cemetery_name,
    dist_pin_to_cemetery.nearest_cemetery_dist_ft,
    dist_pin_to_cemetery.nearest_cemetery_data_year,

    dist_pin_to_cta_route.nearest_cta_route_id,
    dist_pin_to_cta_route.nearest_cta_route_name,
    dist_pin_to_cta_route.nearest_cta_route_dist_ft,
    dist_pin_to_cta_route.nearest_cta_route_data_year,

    dist_pin_to_cta_stop.nearest_cta_stop_id,
    dist_pin_to_cta_stop.nearest_cta_stop_name,
    dist_pin_to_cta_stop.nearest_cta_stop_dist_ft,
    dist_pin_to_cta_stop.nearest_cta_stop_data_year,

    dist_pin_to_golf_course.nearest_golf_course_id,
    dist_pin_to_golf_course.nearest_golf_course_dist_ft,
    dist_pin_to_golf_course.nearest_golf_course_data_year,

    dist_pin_to_hospital.nearest_hospital_gnis_code,
    dist_pin_to_hospital.nearest_hospital_name,
    dist_pin_to_hospital.nearest_hospital_dist_ft,
    dist_pin_to_hospital.nearest_hospital_data_year,

    dist_pin_to_lake_michigan.lake_michigan_dist_ft,
    dist_pin_to_lake_michigan.lake_michigan_data_year,

    dist_pin_to_major_road.nearest_major_road_osm_id,
    dist_pin_to_major_road.nearest_major_road_name,
    dist_pin_to_major_road.nearest_major_road_dist_ft,
    dist_pin_to_major_road.nearest_major_road_data_year,

    dist_pin_to_metra_route.nearest_metra_route_id,
    dist_pin_to_metra_route.nearest_metra_route_name,
    dist_pin_to_metra_route.nearest_metra_route_dist_ft,
    dist_pin_to_metra_route.nearest_metra_route_data_year,

    dist_pin_to_metra_stop.nearest_metra_stop_id,
    dist_pin_to_metra_stop.nearest_metra_stop_name,
    dist_pin_to_metra_stop.nearest_metra_stop_dist_ft,
    dist_pin_to_metra_stop.nearest_metra_stop_data_year,

    dist_pin_to_park.nearest_park_osm_id,
    dist_pin_to_park.nearest_park_name,
    dist_pin_to_park.nearest_park_dist_ft,
    dist_pin_to_park.nearest_park_data_year,

    dist_pin_to_railroad.nearest_railroad_id,
    dist_pin_to_railroad.nearest_railroad_name,
    dist_pin_to_railroad.nearest_railroad_dist_ft,
    dist_pin_to_railroad.nearest_railroad_data_year,

    dist_pin_to_water.nearest_water_id,
    dist_pin_to_water.nearest_water_name,
    dist_pin_to_water.nearest_water_dist_ft,
    dist_pin_to_water.nearest_water_data_year,

    dist_pin_to_pin.nearest_neighbor_1_pin10,
    dist_pin_to_pin.nearest_neighbor_1_dist_ft,
    dist_pin_to_pin.nearest_neighbor_2_pin10,
    dist_pin_to_pin.nearest_neighbor_2_dist_ft,
    dist_pin_to_pin.nearest_neighbor_3_pin10,
    dist_pin_to_pin.nearest_neighbor_3_dist_ft

FROM {{ ref('parcel') }} AS pin
INNER JOIN {{ ref('proximity_crosswalk_year_fill') }} AS cyf
    ON pin.year = cyf.year
LEFT JOIN {{ ref('cnt_pin_num_bus_stop') }} AS cnt_pin_num_bus_stop
    ON pin.pin10 = cnt_pin_num_bus_stop.pin10
    AND cyf.num_bus_stop_data_year = cnt_pin_num_bus_stop.year
LEFT JOIN {{ ref('cnt_pin_num_foreclosure') }} AS cnt_pin_num_foreclosure
    ON pin.pin10 = cnt_pin_num_foreclosure.pin10
    AND cyf.num_foreclosure_data_year = cnt_pin_num_foreclosure.year
LEFT JOIN {{ ref('cnt_pin_num_school') }} AS num_school
    ON pin.pin10 = num_school.pin10
    AND cyf.num_school_data_year = num_school.year
LEFT JOIN {{ ref('cnt_pin_num_school') }} AS num_school_rating
    ON pin.pin10 = num_school_rating.pin10
    AND cyf.num_school_rating_data_year = num_school_rating.year
LEFT JOIN {{ ref('dist_pin_to_bike_trail') }} AS dist_pin_to_bike_trail
    ON pin.pin10 = dist_pin_to_bike_trail.pin10
    AND cyf.nearest_bike_trail_data_year = dist_pin_to_bike_trail.year
LEFT JOIN {{ ref('dist_pin_to_cemetery') }} AS dist_pin_to_cemetery
    ON pin.pin10 = dist_pin_to_cemetery.pin10
    AND cyf.nearest_cemetery_data_year = dist_pin_to_cemetery.year
LEFT JOIN {{ ref('dist_pin_to_cta_route') }} AS dist_pin_to_cta_route
    ON pin.pin10 = dist_pin_to_cta_route.pin10
    AND cyf.nearest_cta_route_data_year = dist_pin_to_cta_route.year
LEFT JOIN {{ ref('dist_pin_to_cta_stop') }} AS dist_pin_to_cta_stop
    ON pin.pin10 = dist_pin_to_cta_stop.pin10
    AND cyf.nearest_cta_stop_data_year = dist_pin_to_cta_stop.year
LEFT JOIN {{ ref('dist_pin_to_golf_course') }} AS dist_pin_to_golf_course
    ON pin.pin10 = dist_pin_to_golf_course.pin10
    AND cyf.nearest_golf_course_data_year = dist_pin_to_golf_course.year
LEFT JOIN {{ ref('dist_pin_to_hospital') }} AS dist_pin_to_hospital
    ON pin.pin10 = dist_pin_to_hospital.pin10
    AND cyf.nearest_hospital_data_year = dist_pin_to_hospital.year
LEFT JOIN {{ ref('dist_pin_to_lake_michigan') }} AS dist_pin_to_lake_michigan
    ON pin.pin10 = dist_pin_to_lake_michigan.pin10
    AND cyf.lake_michigan_data_year = dist_pin_to_lake_michigan.year
LEFT JOIN {{ ref('dist_pin_to_major_road') }} AS dist_pin_to_major_road
    ON pin.pin10 = dist_pin_to_major_road.pin10
    AND cyf.nearest_major_road_data_year = dist_pin_to_major_road.year
LEFT JOIN {{ ref('dist_pin_to_metra_route') }} AS dist_pin_to_metra_route
    ON pin.pin10 = dist_pin_to_metra_route.pin10
    AND cyf.nearest_metra_route_data_year = dist_pin_to_metra_route.year
LEFT JOIN {{ ref('dist_pin_to_metra_stop') }} AS dist_pin_to_metra_stop
    ON pin.pin10 = dist_pin_to_metra_stop.pin10
    AND cyf.nearest_metra_stop_data_year = dist_pin_to_metra_stop.year
LEFT JOIN {{ ref('dist_pin_to_park') }} AS dist_pin_to_park
    ON pin.pin10 = dist_pin_to_park.pin10
    AND cyf.nearest_park_data_year = dist_pin_to_park.year
LEFT JOIN {{ ref('dist_pin_to_pin') }} AS dist_pin_to_pin
    ON pin.pin10 = dist_pin_to_pin.pin10
    AND cyf.year = dist_pin_to_pin.year -- NOTE, doesn't need to be filled
LEFT JOIN {{ ref('dist_pin_to_railroad') }} AS dist_pin_to_railroad
    ON pin.pin10 = dist_pin_to_railroad.pin10
    AND cyf.nearest_railroad_data_year = dist_pin_to_railroad.year
LEFT JOIN {{ ref('dist_pin_to_water') }} AS dist_pin_to_water
    ON pin.pin10 = dist_pin_to_water.pin10
    AND cyf.nearest_water_data_year = dist_pin_to_water.year
