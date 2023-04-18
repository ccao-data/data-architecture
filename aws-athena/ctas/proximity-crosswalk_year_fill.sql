/*
Table detailing which years of proximity data are available and should be joined to each year
of assessment data. Assessment years missing equivalent proximity data are filled thus:

1. All historical data is filled FORWARD in time, i.e. data from 2020 fills
   2021.
2. Current data is filled BACKWARD to account for missing historical data.
*/
CREATE TABLE IF NOT EXISTS proximity.crosswalk_year_fill
WITH (
    format='Parquet',
    write_compression = 'SNAPPY',
    external_location='s3://ccao-athena-ctas-us-east-1/proximity/crosswalk_year_fill'
) AS (
    WITH unfilled AS (
        SELECT
            pin.year,
            Max(num_bus_stop_data_year) AS num_bus_stop_data_year,
            Max(num_foreclosure_data_year) AS num_foreclosure_data_year,
            Max(num_school_data_year) AS num_school_data_year,
            Max(num_school_rating_data_year) AS num_school_rating_data_year,
            Max(nearest_bike_trail_data_year) AS nearest_bike_trail_data_year,
            Max(nearest_cemetery_data_year) AS nearest_cemetery_data_year,
            Max(nearest_cta_route_data_year) AS nearest_cta_route_data_year,
            Max(nearest_cta_stop_data_year) AS nearest_cta_stop_data_year,
            Max(nearest_golf_course_data_year) AS nearest_golf_course_data_year,
            Max(nearest_hospital_data_year) AS nearest_hospital_data_year,
            Max(lake_michigan_data_year) AS lake_michigan_data_year,
            Max(nearest_major_road_data_year) AS nearest_major_road_data_year,
            Max(nearest_metra_route_data_year) AS nearest_metra_route_data_year,
            Max(nearest_metra_stop_data_year) AS nearest_metra_stop_data_year,
            Max(nearest_park_data_year) AS nearest_park_data_year,
            Max(nearest_railroad_data_year) AS nearest_railroad_data_year,
            Max(nearest_water_data_year) AS nearest_water_data_year

        FROM (SELECT DISTINCT year FROM spatial.parcel) pin
        LEFT JOIN (
            SELECT DISTINCT year, num_bus_stop_data_year FROM proximity.cnt_pin_num_bus_stop
            ) cnt_pin_num_bus_stop on pin.year = cnt_pin_num_bus_stop.year
        LEFT JOIN (
            SELECT DISTINCT
                year,
                -- Foreclosure data year descripes a range of years, which can't be used for joins
                SUBSTR(num_foreclosure_data_year, 8, 11) AS num_foreclosure_data_year
            FROM proximity.cnt_pin_num_foreclosure
            ) cnt_pin_num_foreclosure on pin.year = cnt_pin_num_foreclosure.year
        LEFT JOIN (
            SELECT DISTINCT year, num_school_data_year, num_school_rating_data_year FROM proximity.cnt_pin_num_school
            ) cnt_pin_num_school on pin.year = cnt_pin_num_school.year
        LEFT JOIN (
            SELECT DISTINCT year, nearest_bike_trail_data_year FROM proximity.dist_pin_to_bike_trail
            ) dist_pin_to_bike_trail on pin.year = dist_pin_to_bike_trail.year
        LEFT JOIN (
            SELECT DISTINCT year, nearest_cemetery_data_year FROM proximity.dist_pin_to_cemetery
            ) dist_pin_to_cemetery on pin.year = dist_pin_to_cemetery.year
        LEFT JOIN (
            SELECT DISTINCT year, nearest_cta_route_data_year FROM proximity.dist_pin_to_cta_route
            ) dist_pin_to_cta_route on pin.year = dist_pin_to_cta_route.year
        LEFT JOIN (
            SELECT DISTINCT year, nearest_cta_stop_data_year FROM proximity.dist_pin_to_cta_stop
            ) dist_pin_to_cta_stop on pin.year = dist_pin_to_cta_stop.year
        LEFT JOIN (
            SELECT DISTINCT year, nearest_golf_course_data_year FROM proximity.dist_pin_to_golf_course
            ) dist_pin_to_golf_course on pin.year = dist_pin_to_golf_course.year
        LEFT JOIN (
            SELECT DISTINCT year, nearest_hospital_data_year FROM proximity.dist_pin_to_hospital
            ) dist_pin_to_hospital on pin.year = dist_pin_to_hospital.year
        LEFT JOIN (
            SELECT DISTINCT year, lake_michigan_data_year FROM proximity.dist_pin_to_lake_michigan
            ) dist_pin_to_lake_michigan on pin.year = dist_pin_to_lake_michigan.year
        LEFT JOIN (
            SELECT DISTINCT year, nearest_major_road_data_year FROM proximity.dist_pin_to_major_road
            ) dist_pin_to_major_road on pin.year = dist_pin_to_major_road.year
        LEFT JOIN (
            SELECT DISTINCT year, nearest_metra_route_data_year FROM proximity.dist_pin_to_metra_route
            ) dist_pin_to_metra_route on pin.year = dist_pin_to_metra_route.year
        LEFT JOIN (
            SELECT DISTINCT year, nearest_metra_stop_data_year FROM proximity.dist_pin_to_metra_stop
            ) dist_pin_to_metra_stop on pin.year = dist_pin_to_metra_stop.year
        LEFT JOIN (
            SELECT DISTINCT year, nearest_park_data_year FROM proximity.dist_pin_to_park
            ) dist_pin_to_park on pin.year = dist_pin_to_park.year
        LEFT JOIN (
            SELECT DISTINCT year, nearest_railroad_data_year FROM proximity.dist_pin_to_railroad
            ) dist_pin_to_railroad on pin.year = dist_pin_to_railroad.year
        LEFT JOIN (
            SELECT DISTINCT year, nearest_water_data_year FROM proximity.dist_pin_to_water
            ) dist_pin_to_water on pin.year = dist_pin_to_water.year

        GROUP BY pin.year
    )
SELECT
    year,
    CASE
        WHEN num_bus_stop_data_year IS NULL THEN
            LAST_VALUE(num_bus_stop_data_year) IGNORE NULLS
            OVER (ORDER BY year DESC)
        ELSE num_bus_stop_data_year END AS num_bus_stop_data_year,
    CASE
        WHEN num_foreclosure_data_year IS NULL THEN
            LAST_VALUE(num_foreclosure_data_year) IGNORE NULLS
            OVER (ORDER BY year DESC)
        ELSE num_foreclosure_data_year END AS num_foreclosure_data_year,
    CASE
        WHEN num_school_data_year IS NULL THEN
            LAST_VALUE(num_school_data_year) IGNORE NULLS
            OVER (ORDER BY year DESC)
        ELSE num_school_data_year END AS num_school_data_year,
    CASE
        WHEN num_school_rating_data_year IS NULL THEN
            LAST_VALUE(num_school_rating_data_year) IGNORE NULLS
            OVER (ORDER BY year DESC)
        ELSE num_school_rating_data_year END AS num_school_rating_data_year,
    CASE
        WHEN nearest_bike_trail_data_year IS NULL THEN
            LAST_VALUE(nearest_bike_trail_data_year) IGNORE NULLS
            OVER (ORDER BY year DESC)
        ELSE nearest_bike_trail_data_year END AS nearest_bike_trail_data_year,
    CASE
        WHEN nearest_cemetery_data_year IS NULL THEN
            LAST_VALUE(nearest_cemetery_data_year) IGNORE NULLS
            OVER (ORDER BY year DESC)
        ELSE nearest_cemetery_data_year END AS nearest_cemetery_data_year,
    CASE
        WHEN nearest_cta_route_data_year IS NULL THEN
            LAST_VALUE(nearest_cta_route_data_year) IGNORE NULLS
            OVER (ORDER BY year DESC)
        ELSE nearest_cta_route_data_year END AS nearest_cta_route_data_year,
    CASE
        WHEN nearest_cta_stop_data_year IS NULL THEN
            LAST_VALUE(nearest_cta_stop_data_year) IGNORE NULLS
            OVER (ORDER BY year DESC)
        ELSE nearest_cta_stop_data_year END AS nearest_cta_stop_data_year,
    CASE
        WHEN nearest_golf_course_data_year IS NULL THEN
            LAST_VALUE(nearest_golf_course_data_year) IGNORE NULLS
            OVER (ORDER BY year DESC)
        ELSE nearest_golf_course_data_year END AS nearest_golf_course_data_year,
    CASE
        WHEN nearest_hospital_data_year IS NULL THEN
            LAST_VALUE(nearest_hospital_data_year) IGNORE NULLS
            OVER (ORDER BY year DESC)
        ELSE nearest_hospital_data_year END AS nearest_hospital_data_year,
    CASE
        WHEN lake_michigan_data_year IS NULL THEN
            LAST_VALUE(lake_michigan_data_year) IGNORE NULLS
            OVER (ORDER BY year DESC)
        ELSE lake_michigan_data_year END AS lake_michigan_data_year,
    CASE
        WHEN nearest_major_road_data_year IS NULL THEN
            LAST_VALUE(nearest_major_road_data_year) IGNORE NULLS
            OVER (ORDER BY year DESC)
        ELSE nearest_major_road_data_year END AS nearest_major_road_data_year,
    CASE
        WHEN nearest_metra_route_data_year IS NULL THEN
            LAST_VALUE(nearest_metra_route_data_year) IGNORE NULLS
            OVER (ORDER BY year DESC)
        ELSE nearest_metra_route_data_year END AS nearest_metra_route_data_year,
    CASE
        WHEN nearest_metra_stop_data_year IS NULL THEN
            LAST_VALUE(nearest_metra_stop_data_year) IGNORE NULLS
            OVER (ORDER BY year DESC)
        ELSE nearest_metra_stop_data_year END AS nearest_metra_stop_data_year,
    CASE
        WHEN nearest_park_data_year IS NULL THEN
            LAST_VALUE(nearest_park_data_year) IGNORE NULLS
            OVER (ORDER BY year DESC)
        ELSE nearest_park_data_year END AS nearest_park_data_year,
    CASE
        WHEN nearest_railroad_data_year IS NULL THEN
            LAST_VALUE(nearest_railroad_data_year) IGNORE NULLS
            OVER (ORDER BY year DESC)
        ELSE nearest_railroad_data_year END AS nearest_railroad_data_year,
    CASE
        WHEN nearest_water_data_year IS NULL THEN
            LAST_VALUE(nearest_water_data_year) IGNORE NULLS
            OVER (ORDER BY year DESC)
        ELSE nearest_water_data_year END AS nearest_water_data_year

FROM unfilled
ORDER BY YEAR
)