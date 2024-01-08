/*
Table detailing which years of location data are available and
should be joined to each year of assessment data. Assessment years missing
equivalent location data are filled thus:

1. All historical data is filled FORWARD in time, i.e. data from 2020 fills
   2021.
2. Current data is filled BACKWARD to account for missing historical data.
*/
{{ config(materialized='table') }}

WITH unfilled AS (
    SELECT
        pin.year,
        MAX(census.census_data_year)
            AS census_data_year,
        MAX(census_acs5.census_acs5_data_year)
            AS census_acs5_data_year,
        MAX(political.cook_board_of_review_district_data_year)
            AS cook_board_of_review_district_data_year,
        MAX(political.cook_commissioner_district_data_year)
            AS cook_commissioner_district_data_year,
        MAX(political.cook_judicial_district_data_year)
            AS cook_judicial_district_data_year,
        MAX(political.ward_chicago_data_year)
            AS ward_chicago_data_year,
        MAX(political.ward_evanston_data_year)
            AS ward_evanston_data_year,
        MAX(chicago.chicago_community_area_data_year)
            AS chicago_community_area_data_year,
        MAX(chicago.chicago_industrial_corridor_data_year)
            AS chicago_industrial_corridor_data_year,
        MAX(chicago.chicago_police_district_data_year)
            AS chicago_police_district_data_year,
        MAX(economy.econ_coordinated_care_area_data_year)
            AS econ_coordinated_care_area_data_year,
        MAX(economy.econ_enterprise_zone_data_year)
            AS econ_enterprise_zone_data_year,
        MAX(economy.econ_industrial_growth_zone_data_year)
            AS econ_industrial_growth_zone_data_year,
        MAX(economy.econ_qualified_opportunity_zone_data_year)
            AS econ_qualified_opportunity_zone_data_year,
        MAX(environment.env_flood_fema_data_year)
            AS env_flood_fema_data_year,
        MAX(environment.env_flood_fs_data_year)
            AS env_flood_fs_data_year,
        MAX(environment.env_ohare_noise_contour_data_year)
            AS env_ohare_noise_contour_data_year,
        MAX(
            CASE
                WHEN
                    environment.env_airport_noise_data_year != 'omp'
                    THEN environment.env_airport_noise_data_year
            END
        )
            AS env_airport_noise_data_year,
        MAX(school.school_data_year)
            AS school_data_year,
        MAX(tax.tax_data_year)
            AS tax_data_year,
        MAX(access.access_cmap_walk_data_year)
            AS access_cmap_walk_data_year,
        MAX(other.misc_subdivision_data_year)
            AS misc_subdivision_data_year

    FROM (
        SELECT DISTINCT year
        FROM {{ source('spatial', 'parcel') }}
    ) AS pin
    LEFT JOIN (
        SELECT DISTINCT
            year,
            census_data_year
        FROM {{ ref('location.census') }}
    ) AS census ON pin.year = census.year
    LEFT JOIN (
        SELECT DISTINCT
            year,
            census_acs5_data_year
        FROM {{ ref('location.census_acs5') }}
    ) AS census_acs5 ON pin.year = census_acs5.year
    LEFT JOIN (
        SELECT DISTINCT
            year,
            cook_board_of_review_district_data_year,
            cook_commissioner_district_data_year,
            cook_judicial_district_data_year,
            ward_chicago_data_year,
            ward_evanston_data_year
        FROM {{ ref('location.political') }}
    ) AS political ON pin.year = political.year
    LEFT JOIN (
        SELECT DISTINCT
            year,
            chicago_community_area_data_year,
            chicago_industrial_corridor_data_year,
            chicago_police_district_data_year
        FROM {{ ref('location.chicago') }}
    ) AS chicago ON pin.year = chicago.year
    LEFT JOIN (
        SELECT DISTINCT
            year,
            econ_coordinated_care_area_data_year,
            econ_enterprise_zone_data_year,
            econ_industrial_growth_zone_data_year,
            econ_qualified_opportunity_zone_data_year
        FROM {{ ref('location.economy') }}
    ) AS economy ON pin.year = economy.year
    LEFT JOIN (
        SELECT DISTINCT
            year,
            env_flood_fema_data_year,
            env_flood_fs_data_year,
            env_ohare_noise_contour_data_year,
            env_airport_noise_data_year
        FROM {{ ref('location.environment') }}
    ) AS environment ON pin.year = environment.year
    LEFT JOIN (
        SELECT DISTINCT
            year,
            school_data_year
        FROM {{ ref('location.school') }}
    ) AS school ON pin.year = school.year
    LEFT JOIN (
        SELECT DISTINCT
            year,
            tax_data_year
        FROM {{ ref('location.tax') }}
    ) AS tax ON pin.year = tax.year
    LEFT JOIN (
        SELECT DISTINCT
            year,
            access_cmap_walk_data_year
        FROM {{ ref('location.access') }}
    ) AS access ON pin.year = access.year
    LEFT JOIN (
        SELECT DISTINCT
            year,
            misc_subdivision_data_year
        FROM {{ ref('location.other') }}
    ) AS other ON pin.year = other.year
    GROUP BY pin.year
)

SELECT
    unfilled.year,
    COALESCE(
        census_data_year, LAST_VALUE(census_data_year)
            IGNORE NULLS
            OVER (ORDER BY unfilled.year DESC)
    ) AS census_data_year,
    COALESCE(
        census_acs5_data_year, LAST_VALUE(census_acs5_data_year)
            IGNORE NULLS
            OVER (ORDER BY unfilled.year DESC)
    ) AS census_acs5_data_year,
    COALESCE(
        cook_board_of_review_district_data_year,
        LAST_VALUE(cook_board_of_review_district_data_year)
            IGNORE NULLS
            OVER (ORDER BY unfilled.year DESC)
    ) AS cook_board_of_review_district_data_year,
    COALESCE(
        cook_commissioner_district_data_year,
        LAST_VALUE(cook_commissioner_district_data_year)
            IGNORE NULLS
            OVER (ORDER BY unfilled.year DESC)
    ) AS cook_commissioner_district_data_year,
    COALESCE(
        cook_judicial_district_data_year,
        LAST_VALUE(cook_judicial_district_data_year)
            IGNORE NULLS
            OVER (ORDER BY unfilled.year DESC)
    ) AS cook_judicial_district_data_year,
    COALESCE(
        ward_chicago_data_year, LAST_VALUE(ward_chicago_data_year)
            IGNORE NULLS
            OVER (ORDER BY unfilled.year DESC)
    ) AS ward_chicago_data_year,
    COALESCE(
        ward_evanston_data_year, LAST_VALUE(ward_evanston_data_year)
            IGNORE NULLS
            OVER (ORDER BY unfilled.year DESC)
    ) AS ward_evanston_data_year,
    COALESCE(
        chicago_community_area_data_year,
        LAST_VALUE(chicago_community_area_data_year)
            IGNORE NULLS
            OVER (ORDER BY unfilled.year DESC)
    ) AS chicago_community_area_data_year,
    COALESCE(
        chicago_industrial_corridor_data_year,
        LAST_VALUE(chicago_industrial_corridor_data_year)
            IGNORE NULLS
            OVER (ORDER BY unfilled.year DESC)
    ) AS chicago_industrial_corridor_data_year,
    COALESCE(
        chicago_police_district_data_year,
        LAST_VALUE(chicago_police_district_data_year)
            IGNORE NULLS
            OVER (ORDER BY unfilled.year DESC)
    ) AS chicago_police_district_data_year,
    COALESCE(
        econ_coordinated_care_area_data_year,
        LAST_VALUE(econ_coordinated_care_area_data_year)
            IGNORE NULLS
            OVER (ORDER BY unfilled.year DESC)
    ) AS econ_coordinated_care_area_data_year,
    COALESCE(
        econ_enterprise_zone_data_year,
        LAST_VALUE(econ_enterprise_zone_data_year)
            IGNORE NULLS
            OVER (ORDER BY unfilled.year DESC)
    ) AS econ_enterprise_zone_data_year,
    COALESCE(
        econ_industrial_growth_zone_data_year,
        LAST_VALUE(econ_industrial_growth_zone_data_year)
            IGNORE NULLS
            OVER (ORDER BY unfilled.year DESC)
    ) AS econ_industrial_growth_zone_data_year,
    COALESCE(
        econ_qualified_opportunity_zone_data_year,
        LAST_VALUE(econ_qualified_opportunity_zone_data_year)
            IGNORE NULLS
            OVER (ORDER BY unfilled.year DESC)
    ) AS econ_qualified_opportunity_zone_data_year,
    COALESCE(
        env_flood_fema_data_year, LAST_VALUE(env_flood_fema_data_year)
            IGNORE NULLS
            OVER (ORDER BY unfilled.year DESC)
    ) AS env_flood_fema_data_year,
    COALESCE(
        env_flood_fs_data_year, LAST_VALUE(env_flood_fs_data_year)
            IGNORE NULLS
            OVER (ORDER BY unfilled.year DESC)
    ) AS env_flood_fs_data_year,
    COALESCE(
        env_ohare_noise_contour_data_year,
        LAST_VALUE(env_ohare_noise_contour_data_year)
            IGNORE NULLS
            OVER (ORDER BY unfilled.year DESC)
    ) AS env_ohare_noise_contour_data_year,
    COALESCE(
        env_airport_noise_data_year,
        LAST_VALUE(env_airport_noise_data_year)
            IGNORE NULLS
            OVER (ORDER BY unfilled.year DESC)
    ) AS env_airport_noise_data_year,
    COALESCE(
        school_data_year, LAST_VALUE(school_data_year)
            IGNORE NULLS
            OVER (ORDER BY unfilled.year DESC)
    ) AS school_data_year,
    COALESCE(
        tax_data_year, LAST_VALUE(tax_data_year)
            IGNORE NULLS
            OVER (ORDER BY unfilled.year DESC)
    ) AS tax_data_year,
    COALESCE(
        access_cmap_walk_data_year,
        LAST_VALUE(access_cmap_walk_data_year)
            IGNORE NULLS
            OVER (ORDER BY unfilled.year DESC)
    ) AS access_cmap_walk_data_year,
    COALESCE(
        misc_subdivision_data_year,
        LAST_VALUE(misc_subdivision_data_year)
            IGNORE NULLS
            OVER (ORDER BY unfilled.year DESC)
    ) AS misc_subdivision_data_year
FROM unfilled
ORDER BY unfilled.year
