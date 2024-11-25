/*
Table detailing which years of proximity data are available and
should be joined to each year of assessment data. Assessment years
missing equivalent proximity data are filled thus:

1. All historical data is filled FORWARD in time, i.e. data from 2020 fills
   2021.
2. Current data is filled BACKWARD to account for missing historical data.
*/
{{ config(materialized='table') }}

WITH unfilled AS (
    SELECT
        pin.year,
        MAX(cnt_pin_num_bus_stop.num_bus_stop_data_year)
            AS num_bus_stop_data_year,
        MAX(cnt_pin_num_foreclosure.num_foreclosure_data_year)
            AS num_foreclosure_data_year,
        MAX(cnt_pin_num_school.num_school_data_year)
            AS num_school_data_year,
        MAX(cnt_pin_num_school.num_school_rating_data_year)
            AS num_school_rating_data_year,
        MAX(dist_pin_to_airport.airport_data_year)
            AS airport_data_year,
        MAX(dist_pin_to_bike_trail.nearest_bike_trail_data_year)
            AS nearest_bike_trail_data_year,
        MAX(dist_pin_to_cemetery.nearest_cemetery_data_year)
            AS nearest_cemetery_data_year,
        MAX(dist_pin_to_cta_route.nearest_cta_route_data_year)
            AS nearest_cta_route_data_year,
        MAX(dist_pin_to_cta_stop.nearest_cta_stop_data_year)
            AS nearest_cta_stop_data_year,
        MAX(dist_pin_to_golf_course.nearest_golf_course_data_year)
            AS nearest_golf_course_data_year,
        MAX(dist_pin_to_grocery_store.nearest_grocery_store_data_year)
            AS nearest_grocery_store_data_year,
        MAX(dist_pin_to_hospital.nearest_hospital_data_year)
            AS nearest_hospital_data_year,
        MAX(dist_pin_to_lake_michigan.lake_michigan_data_year)
            AS lake_michigan_data_year,
        MAX(dist_pin_to_major_road.nearest_major_road_data_year)
            AS nearest_major_road_data_year,
        MAX(dist_pin_to_metra_route.nearest_metra_route_data_year)
            AS nearest_metra_route_data_year,
        MAX(dist_pin_to_metra_stop.nearest_metra_stop_data_year)
            AS nearest_metra_stop_data_year,
        MAX(dist_pin_to_new_construction.nearest_new_construction_data_year)
            AS nearest_new_construction_data_year,
        MAX(dist_pin_to_park.nearest_park_data_year)
            AS nearest_park_data_year,
        MAX(dist_pin_to_railroad.nearest_railroad_data_year)
            AS nearest_railroad_data_year,
        MAX(dist_pin_to_road_arterial.nearest_road_arterial_data_year)
            AS nearest_road_arterial_data_year,
        MAX(dist_pin_to_road_collector.nearest_road_collector_data_year)
            AS nearest_road_collector_data_year,
        MAX(dist_pin_to_road_highway.nearest_road_highway_data_year)
            AS nearest_road_highway_data_year,
        MAX(dist_pin_to_secondary_road.nearest_secondary_road_data_year)
            AS nearest_secondary_road_data_year,
        MAX(dist_pin_to_stadium.nearest_stadium_data_year)
            AS nearest_stadium_data_year,
        MAX(dist_pin_to_university.nearest_university_data_year)
            AS nearest_university_data_year,
        MAX(dist_pin_to_vacant_land.nearest_vacant_land_data_year)
            AS nearest_vacant_land_data_year,
        MAX(dist_pin_to_water.nearest_water_data_year)
            AS nearest_water_data_year
    FROM
        (SELECT DISTINCT year FROM {{ source('spatial', 'parcel') }}) AS pin
    LEFT JOIN (
        SELECT DISTINCT
            year,
            num_bus_stop_data_year
        FROM {{ ref('proximity.cnt_pin_num_bus_stop') }}
    ) AS cnt_pin_num_bus_stop ON pin.year = cnt_pin_num_bus_stop.year
    LEFT JOIN (
        SELECT DISTINCT
            year,
            -- Foreclosure data year descripes a range of years,
            -- which can't be used for joins
            SUBSTR(num_foreclosure_data_year, 8, 11)
                AS num_foreclosure_data_year
        FROM {{ ref('proximity.cnt_pin_num_foreclosure') }}
    ) AS cnt_pin_num_foreclosure ON pin.year = cnt_pin_num_foreclosure.year
    LEFT JOIN (
        SELECT DISTINCT
            year,
            num_school_data_year,
            num_school_rating_data_year
        FROM {{ ref('proximity.cnt_pin_num_school') }}
    ) AS cnt_pin_num_school ON pin.year = cnt_pin_num_school.year
    LEFT JOIN (
        SELECT DISTINCT
            year,
            airport_data_year
        FROM {{ ref('proximity.dist_pin_to_airport' ) }}
    ) AS dist_pin_to_airport ON pin.year = dist_pin_to_airport.year
    LEFT JOIN (
        SELECT DISTINCT
            year,
            nearest_bike_trail_data_year
        FROM {{ ref('proximity.dist_pin_to_bike_trail') }}
    ) AS dist_pin_to_bike_trail ON pin.year = dist_pin_to_bike_trail.year
    LEFT JOIN (
        SELECT DISTINCT
            year,
            nearest_cemetery_data_year
        FROM {{ ref('proximity.dist_pin_to_cemetery') }}
    ) AS dist_pin_to_cemetery ON pin.year = dist_pin_to_cemetery.year
    LEFT JOIN (
        SELECT DISTINCT
            year,
            nearest_cta_route_data_year
        FROM {{ ref('proximity.dist_pin_to_cta_route') }}
    ) AS dist_pin_to_cta_route ON pin.year = dist_pin_to_cta_route.year
    LEFT JOIN (
        SELECT DISTINCT
            year,
            nearest_cta_stop_data_year
        FROM {{ ref('proximity.dist_pin_to_cta_stop') }}
    ) AS dist_pin_to_cta_stop ON pin.year = dist_pin_to_cta_stop.year
    LEFT JOIN (
        SELECT DISTINCT
            year,
            nearest_golf_course_data_year
        FROM {{ ref('proximity.dist_pin_to_golf_course') }}
    ) AS dist_pin_to_golf_course ON pin.year = dist_pin_to_golf_course.year
    LEFT JOIN (
        SELECT DISTINCT
            year,
            nearest_grocery_store_data_year
        FROM {{ ref('proximity.dist_pin_to_grocery_store') }}
    ) AS dist_pin_to_grocery_store ON pin.year = dist_pin_to_grocery_store.year
    LEFT JOIN (
        SELECT DISTINCT
            year,
            nearest_hospital_data_year
        FROM {{ ref('proximity.dist_pin_to_hospital') }}
    ) AS dist_pin_to_hospital ON pin.year = dist_pin_to_hospital.year
    LEFT JOIN (
        SELECT DISTINCT
            year,
            lake_michigan_data_year
        FROM {{ ref('proximity.dist_pin_to_lake_michigan') }}
    ) AS dist_pin_to_lake_michigan
        ON pin.year = dist_pin_to_lake_michigan.year
    LEFT JOIN (
        SELECT DISTINCT
            year,
            nearest_major_road_data_year
        FROM {{ ref('proximity.dist_pin_to_major_road') }}
    ) AS dist_pin_to_major_road ON pin.year = dist_pin_to_major_road.year
    LEFT JOIN (
        SELECT DISTINCT
            year,
            nearest_metra_route_data_year
        FROM {{ ref('proximity.dist_pin_to_metra_route') }}
    ) AS dist_pin_to_metra_route ON pin.year = dist_pin_to_metra_route.year
    LEFT JOIN (
        SELECT DISTINCT
            year,
            nearest_metra_stop_data_year
        FROM {{ ref('proximity.dist_pin_to_metra_stop') }}
    ) AS dist_pin_to_metra_stop ON pin.year = dist_pin_to_metra_stop.year
    LEFT JOIN (
        SELECT DISTINCT
            year,
            nearest_new_construction_data_year
        FROM {{ ref('proximity.dist_pin_to_new_construction') }}
    ) AS dist_pin_to_new_construction
        ON pin.year = dist_pin_to_new_construction.year
    LEFT JOIN (
        SELECT DISTINCT
            year,
            nearest_park_data_year
        FROM {{ ref('proximity.dist_pin_to_park') }}
    ) AS dist_pin_to_park ON pin.year = dist_pin_to_park.year
    LEFT JOIN (
        SELECT DISTINCT
            year,
            nearest_railroad_data_year
        FROM {{ ref('proximity.dist_pin_to_railroad') }}
    ) AS dist_pin_to_railroad ON pin.year = dist_pin_to_railroad.year
    LEFT JOIN (
        SELECT DISTINCT
            year,
            nearest_road_arterial_data_year
        FROM {{ ref('proximity.dist_pin_to_road_arterial' ) }}
    ) AS dist_pin_to_road_arterial ON pin.year = dist_pin_to_road_arterial.year
    LEFT JOIN (
        SELECT DISTINCT
            year,
            nearest_road_collector_data_year
        FROM {{ ref('proximity.dist_pin_to_road_collector' ) }}
    ) AS dist_pin_to_road_collector
        ON pin.year = dist_pin_to_road_collector.year
    LEFT JOIN (
        SELECT DISTINCT
            year,
            nearest_road_highway_data_year
        FROM {{ ref('proximity.dist_pin_to_road_highway' ) }}
    ) AS dist_pin_to_road_highway ON pin.year = dist_pin_to_road_highway.year
    LEFT JOIN (
        SELECT DISTINCT
            year,
            nearest_secondary_road_data_year
        FROM {{ ref('proximity.dist_pin_to_secondary_road') }}
    ) AS dist_pin_to_secondary_road
        ON pin.year = dist_pin_to_secondary_road.year
    LEFT JOIN (
        SELECT DISTINCT
            year,
            nearest_stadium_data_year
        FROM {{ ref('proximity.dist_pin_to_stadium') }}
    ) AS dist_pin_to_stadium
        ON pin.year = dist_pin_to_stadium.year
    LEFT JOIN (
        SELECT DISTINCT
            year,
            nearest_university_data_year
        FROM {{ ref('proximity.dist_pin_to_university') }}
    ) AS dist_pin_to_university ON pin.year = dist_pin_to_university.year
    LEFT JOIN (
        SELECT DISTINCT
            year,
            nearest_vacant_land_data_year
        FROM {{ ref('proximity.dist_pin_to_vacant_land') }}
    ) AS dist_pin_to_vacant_land ON pin.year = dist_pin_to_vacant_land.year
    LEFT JOIN (
        SELECT DISTINCT
            year,
            nearest_water_data_year
        FROM {{ ref('proximity.dist_pin_to_water') }}
    ) AS dist_pin_to_water ON pin.year = dist_pin_to_water.year

    GROUP BY pin.year
)

SELECT
    year,
    COALESCE(
        num_bus_stop_data_year, LAST_VALUE(num_bus_stop_data_year)
            IGNORE NULLS
            OVER (ORDER BY year DESC)
    ) AS num_bus_stop_data_year,
    COALESCE(
        num_foreclosure_data_year,
        LAST_VALUE(num_foreclosure_data_year)
            IGNORE NULLS
            OVER (ORDER BY year DESC)
    ) AS num_foreclosure_data_year,
    COALESCE(
        num_school_data_year, LAST_VALUE(num_school_data_year)
            IGNORE NULLS
            OVER (ORDER BY year DESC)
    ) AS num_school_data_year,
    COALESCE(
        num_school_rating_data_year,
        LAST_VALUE(num_school_rating_data_year)
            IGNORE NULLS
            OVER (ORDER BY year DESC)
    ) AS num_school_rating_data_year,
    COALESCE(
        airport_data_year,
        LAST_VALUE(airport_data_year)
            IGNORE NULLS
            OVER (ORDER BY year DESC)
    ) AS airport_data_year,
    COALESCE(
        nearest_bike_trail_data_year,
        LAST_VALUE(nearest_bike_trail_data_year)
            IGNORE NULLS
            OVER (ORDER BY year DESC)
    ) AS nearest_bike_trail_data_year,
    COALESCE(
        nearest_cemetery_data_year,
        LAST_VALUE(nearest_cemetery_data_year)
            IGNORE NULLS
            OVER (ORDER BY year DESC)
    ) AS nearest_cemetery_data_year,
    COALESCE(
        nearest_cta_route_data_year,
        LAST_VALUE(nearest_cta_route_data_year)
            IGNORE NULLS
            OVER (ORDER BY year DESC)
    ) AS nearest_cta_route_data_year,
    COALESCE(
        nearest_cta_stop_data_year,
        LAST_VALUE(nearest_cta_stop_data_year)
            IGNORE NULLS
            OVER (ORDER BY year DESC)
    ) AS nearest_cta_stop_data_year,
    COALESCE(
        nearest_golf_course_data_year,
        LAST_VALUE(nearest_golf_course_data_year)
            IGNORE NULLS
            OVER (ORDER BY year DESC)
    ) AS nearest_golf_course_data_year,
    COALESCE(
        nearest_grocery_store_data_year,
        LAST_VALUE(nearest_grocery_store_data_year)
            IGNORE NULLS
            OVER (ORDER BY year DESC)
    ) AS nearest_grocery_store_data_year,
    COALESCE(
        nearest_hospital_data_year,
        LAST_VALUE(nearest_hospital_data_year)
            IGNORE NULLS
            OVER (ORDER BY year DESC)
    ) AS nearest_hospital_data_year,
    COALESCE(
        lake_michigan_data_year, LAST_VALUE(lake_michigan_data_year)
            IGNORE NULLS
            OVER (ORDER BY year DESC)
    ) AS lake_michigan_data_year,
    COALESCE(
        nearest_major_road_data_year,
        LAST_VALUE(nearest_major_road_data_year)
            IGNORE NULLS
            OVER (ORDER BY year DESC)
    ) AS nearest_major_road_data_year,
    COALESCE(
        nearest_metra_route_data_year,
        LAST_VALUE(nearest_metra_route_data_year)
            IGNORE NULLS
            OVER (ORDER BY year DESC)
    ) AS nearest_metra_route_data_year,
    COALESCE(
        nearest_metra_stop_data_year,
        LAST_VALUE(nearest_metra_stop_data_year)
            IGNORE NULLS
            OVER (ORDER BY year DESC)
    ) AS nearest_metra_stop_data_year,
    COALESCE(
        nearest_new_construction_data_year,
        LAST_VALUE(nearest_new_construction_data_year)
            IGNORE NULLS
            OVER (ORDER BY year DESC)
    ) AS nearest_new_construction_data_year,
    COALESCE(
        nearest_park_data_year, LAST_VALUE(nearest_park_data_year)
            IGNORE NULLS
            OVER (ORDER BY year DESC)
    ) AS nearest_park_data_year,
    COALESCE(
        nearest_railroad_data_year,
        LAST_VALUE(nearest_railroad_data_year)
            IGNORE NULLS
            OVER (ORDER BY year DESC)
    ) AS nearest_railroad_data_year,
    COALESCE(
        nearest_road_arterial_data_year,
        LAST_VALUE(nearest_road_arterial_data_year)
            IGNORE NULLS
            OVER (ORDER BY year DESC)
    ) AS nearest_road_arterial_data_year,
    COALESCE(
        nearest_road_collector_data_year,
        LAST_VALUE(nearest_road_collector_data_year)
            IGNORE NULLS
            OVER (ORDER BY year DESC)
    ) AS nearest_road_collector_data_year,
    COALESCE(
        nearest_road_highway_data_year,
        LAST_VALUE(nearest_road_highway_data_year)
            IGNORE NULLS
            OVER (ORDER BY year DESC)
    ) AS nearest_road_highway_data_year,
    COALESCE(
        nearest_secondary_road_data_year,
        LAST_VALUE(nearest_secondary_road_data_year)
            IGNORE NULLS
            OVER (ORDER BY year DESC)
    ) AS nearest_secondary_road_data_year,
    COALESCE(
        nearest_stadium_data_year,
        LAST_VALUE(nearest_stadium_data_year)
            IGNORE NULLS
            OVER (ORDER BY year DESC)
    ) AS nearest_stadium_data_year,
    COALESCE(
        nearest_university_data_year,
        LAST_VALUE(nearest_university_data_year)
            IGNORE NULLS
            OVER (ORDER BY year DESC)
    ) AS nearest_university_data_year,
    COALESCE(
        nearest_vacant_land_data_year,
        LAST_VALUE(nearest_vacant_land_data_year)
            IGNORE NULLS
            OVER (ORDER BY year DESC)
    ) AS nearest_vacant_land_data_year,
    COALESCE(
        nearest_water_data_year, LAST_VALUE(nearest_water_data_year)
            IGNORE NULLS
            OVER (ORDER BY year DESC)
    ) AS nearest_water_data_year

FROM unfilled
ORDER BY year
