/*
Table detailing which years of location data are available and should be joined to each year
of assessment data. Assessment years missing equivalent location data are filled thus:

1. All historical data is filled FORWARD in time, i.e. data from 2020 fills
   2021.
2. Current data is filled BACKWARD to account for missing historical data.
*/
CREATE TABLE IF NOT EXISTS location.crosswalk_year_fill
WITH (
    format='Parquet',
    write_compression = 'SNAPPY',
    external_location='s3://ccao-athena-ctas-us-east-1/location/crosswalk_year_fill'
) AS (
    WITH unfilled AS (
        SELECT
            pin.year,
            Max(census_data_year) AS census_data_year,
            Max(census_acs5_data_year) AS census_acs5_data_year,
            Max(cook_board_of_review_district_data_year) AS cook_board_of_review_district_data_year,
            Max(cook_commissioner_district_data_year) AS cook_commissioner_district_data_year,
            Max(cook_judicial_district_data_year) AS cook_judicial_district_data_year,
            Max(cook_municipality_data_year) AS cook_municipality_data_year,
            Max(
                CASE
                    WHEN chicago_ward_data_year IS NOT NULL THEN chicago_ward_data_year
                    WHEN evanston_ward_data_year IS NOT NULL THEN evanston_ward_data_year
                ELSE NULL END
                ) AS ward_data_year,
            Max(chicago_community_area_data_year) AS chicago_community_area_data_year,
            Max(chicago_industrial_corridor_data_year) AS chicago_industrial_corridor_data_year,
            Max(chicago_police_district_data_year) AS chicago_police_district_data_year,
            Max(econ_coordinated_care_area_data_year) AS econ_coordinated_care_area_data_year,
            Max(econ_enterprise_zone_data_year) AS econ_enterprise_zone_data_year,
            Max(econ_industrial_growth_zone_data_year) AS econ_industrial_growth_zone_data_year,
            Max(econ_qualified_opportunity_zone_data_year) AS econ_qualified_opportunity_zone_data_year,
            Max(env_flood_fema_data_year) AS env_flood_fema_data_year,
            Max(env_flood_fs_data_year) AS env_flood_fs_data_year,
            Max(env_ohare_noise_contour_data_year) AS env_ohare_noise_contour_data_year,
            Max(env_airport_noise_data_year) AS env_airport_noise_data_year,
            Max(school_school_year) AS school_school_year,
            Max(school_data_year) AS school_data_year,
            Max(tax_community_college_district_data_year) AS tax_community_college_district_data_year,
            Max(tax_fire_protection_district_data_year) AS tax_fire_protection_district_data_year,
            Max(tax_library_district_data_year) AS tax_library_district_data_year,
            Max(tax_park_district_data_year) AS tax_park_district_data_year,
            Max(tax_sanitation_district_data_year) AS tax_sanitation_district_data_year,
            Max(tax_special_service_area_data_year) AS tax_special_service_area_data_year,
            Max(tax_tif_district_data_year) AS tax_tif_district_data_year,
            Max(access_cmap_walk_data_year) AS access_cmap_walk_data_year,
            Max(misc_subdivision_data_year) AS misc_subdivision_data_year

        FROM (SELECT DISTINCT year FROM spatial.parcel) pin
        LEFT JOIN (
            SELECT DISTINCT year, census_data_year FROM location.census
            ) census ON pin.year = census.year
        LEFT JOIN (
            SELECT DISTINCT year, census_acs5_data_year FROM location.census_acs5
            ) census_acs5 ON pin.year = census_acs5.year
        LEFT JOIN (
            SELECT DISTINCT
                year,
                cook_board_of_review_district_data_year,
                cook_commissioner_district_data_year,
                cook_judicial_district_data_year,
                cook_municipality_data_year
            FROM location.political
            ) political ON pin.year = political.year
        LEFT JOIN (
            SELECT DISTINCT
                year,
                chicago_ward_data_year,
                chicago_community_area_data_year,
                chicago_industrial_corridor_data_year,
                chicago_police_district_data_year
            FROM location.chicago
            ) chicago ON pin.year = chicago.year
        LEFT JOIN (
            SELECT DISTINCT year, evanston_ward_data_year FROM location.evanston
            ) evanston ON pin.year = evanston.year
        LEFT JOIN (
            SELECT DISTINCT
                year,
                econ_coordinated_care_area_data_year,
                econ_enterprise_zone_data_year,
                econ_industrial_growth_zone_data_year,
                econ_qualified_opportunity_zone_data_year
            FROM location.economy
            ) economy ON pin.year = economy.year
        LEFT JOIN (
            SELECT DISTINCT
                year,
                env_flood_fema_data_year,
                env_flood_fs_data_year,
                env_ohare_noise_contour_data_year,
                env_airport_noise_data_year
            FROM location.environment
            ) environment ON pin.year = environment.year
        LEFT JOIN (
            SELECT DISTINCT
                year,
                school_school_year,
                school_data_year
            FROM location.school
            ) school ON pin.year = school.year
        LEFT JOIN (
            SELECT DISTINCT
                year,
                tax_community_college_district_data_year,
                tax_fire_protection_district_data_year,
                tax_library_district_data_year,
                tax_park_district_data_year,
                tax_sanitation_district_data_year,
                tax_special_service_area_data_year,
                tax_tif_district_data_year
            FROM location.tax
            ) tax ON pin.year = tax.year
        LEFT JOIN (
            SELECT DISTINCT year, access_cmap_walk_data_year FROM location.access
            ) access ON pin.year = access.year
        LEFT JOIN (
            SELECT DISTINCT year, misc_subdivision_data_year FROM location.other
            ) other ON pin.year = other.year

        GROUP BY pin.year
    )
    SELECT
        year,
        CASE
            WHEN census_data_year IS NULL THEN
                LAST_VALUE(census_data_year) IGNORE NULLS
                OVER (ORDER BY year DESC)
            ELSE census_data_year END AS census_data_year,
        CASE
            WHEN census_acs5_data_year IS NULL THEN
                LAST_VALUE(census_acs5_data_year) IGNORE NULLS
                OVER (ORDER BY year DESC)
            ELSE census_acs5_data_year END AS census_acs5_data_year,
        CASE
            WHEN cook_board_of_review_district_data_year IS NULL THEN
                LAST_VALUE(cook_board_of_review_district_data_year) IGNORE NULLS
                OVER (ORDER BY year DESC)
            ELSE cook_board_of_review_district_data_year  END AS cook_board_of_review_district_data_year ,
        CASE
            WHEN cook_commissioner_district_data_year IS NULL THEN
                LAST_VALUE(cook_commissioner_district_data_year) IGNORE NULLS
                OVER (ORDER BY year DESC)
            ELSE cook_commissioner_district_data_year END AS cook_commissioner_district_data_year,
        CASE
            WHEN cook_judicial_district_data_year IS NULL THEN
                LAST_VALUE(cook_judicial_district_data_year) IGNORE NULLS
                OVER (ORDER BY year DESC)
            ELSE cook_judicial_district_data_year END AS cook_judicial_district_data_year,
        CASE
            WHEN cook_municipality_data_year IS NULL THEN
                LAST_VALUE(cook_municipality_data_year) IGNORE NULLS
                OVER (ORDER BY year DESC)
            ELSE cook_municipality_data_year END AS cook_municipality_data_year,
        CASE
            WHEN ward_data_year IS NULL THEN
                LAST_VALUE(ward_data_year) IGNORE NULLS
                OVER (ORDER BY year DESC)
            ELSE ward_data_year END AS ward_data_year,
        CASE
            WHEN chicago_community_area_data_year IS NULL THEN
                LAST_VALUE(chicago_community_area_data_year) IGNORE NULLS
                OVER (ORDER BY year DESC)
            ELSE chicago_community_area_data_year END AS chicago_community_area_data_year,
        CASE
            WHEN chicago_industrial_corridor_data_year IS NULL THEN
                LAST_VALUE(chicago_industrial_corridor_data_year) IGNORE NULLS
                OVER (ORDER BY year DESC)
            ELSE chicago_industrial_corridor_data_year END AS chicago_industrial_corridor_data_year,
        CASE
            WHEN chicago_police_district_data_year IS NULL THEN
                LAST_VALUE(chicago_police_district_data_year) IGNORE NULLS
                OVER (ORDER BY year DESC)
            ELSE chicago_police_district_data_year END AS chicago_police_district_data_year,
        CASE
            WHEN econ_coordinated_care_area_data_year IS NULL THEN
                LAST_VALUE(econ_coordinated_care_area_data_year) IGNORE NULLS
                OVER (ORDER BY year DESC)
            ELSE econ_coordinated_care_area_data_year END AS econ_coordinated_care_area_data_year,
        CASE
            WHEN econ_enterprise_zone_data_year IS NULL THEN
                LAST_VALUE(econ_enterprise_zone_data_year) IGNORE NULLS
                OVER (ORDER BY year DESC)
            ELSE econ_enterprise_zone_data_year END AS econ_enterprise_zone_data_year,
        CASE
            WHEN econ_industrial_growth_zone_data_year IS NULL THEN
                LAST_VALUE(econ_industrial_growth_zone_data_year) IGNORE NULLS
                OVER (ORDER BY year DESC)
            ELSE econ_industrial_growth_zone_data_year END AS econ_industrial_growth_zone_data_year,
        CASE
            WHEN econ_qualified_opportunity_zone_data_year IS NULL THEN
                LAST_VALUE(econ_qualified_opportunity_zone_data_year) IGNORE NULLS
                OVER (ORDER BY year DESC)
            ELSE econ_qualified_opportunity_zone_data_year END AS econ_qualified_opportunity_zone_data_year,
        CASE
            WHEN env_flood_fema_data_year IS NULL THEN
                LAST_VALUE(env_flood_fema_data_year) IGNORE NULLS
                OVER (ORDER BY year DESC)
            ELSE env_flood_fema_data_year END AS env_flood_fema_data_year,
        CASE
            WHEN env_flood_fs_data_year IS NULL THEN
                LAST_VALUE(env_flood_fs_data_year) IGNORE NULLS
                OVER (ORDER BY year DESC)
            ELSE env_flood_fs_data_year END AS env_flood_fs_data_year,
        CASE
            WHEN env_ohare_noise_contour_data_year IS NULL THEN
                LAST_VALUE(env_ohare_noise_contour_data_year) IGNORE NULLS
                OVER (ORDER BY year DESC)
            ELSE env_ohare_noise_contour_data_year END AS env_ohare_noise_contour_data_year,
        CASE
            WHEN env_airport_noise_data_year IS NULL THEN
                LAST_VALUE(env_airport_noise_data_year) IGNORE NULLS
                OVER (ORDER BY year DESC)
            ELSE env_airport_noise_data_year END AS env_airport_noise_data_year,
        CASE
            WHEN school_school_year IS NULL THEN
                LAST_VALUE(school_school_year) IGNORE NULLS
                OVER (ORDER BY year DESC)
            ELSE school_school_year END AS school_school_year,
        CASE
            WHEN school_data_year IS NULL THEN
                LAST_VALUE(school_data_year) IGNORE NULLS
                OVER (ORDER BY year DESC)
            ELSE school_data_year END AS school_data_year,
        CASE
            WHEN tax_community_college_district_data_year IS NULL THEN
                LAST_VALUE(tax_community_college_district_data_year) IGNORE NULLS
                OVER (ORDER BY year DESC)
            ELSE tax_community_college_district_data_year END AS tax_community_college_district_data_year,
        CASE
            WHEN tax_fire_protection_district_data_year IS NULL THEN
                LAST_VALUE(tax_fire_protection_district_data_year) IGNORE NULLS
                OVER (ORDER BY year DESC)
            ELSE tax_fire_protection_district_data_year END AS tax_fire_protection_district_data_year,
        CASE
            WHEN tax_library_district_data_year IS NULL THEN
                LAST_VALUE(tax_library_district_data_year) IGNORE NULLS
                OVER (ORDER BY year DESC)
            ELSE tax_library_district_data_year END AS tax_library_district_data_year,
        CASE
            WHEN tax_park_district_data_year IS NULL THEN
                LAST_VALUE(tax_park_district_data_year) IGNORE NULLS
                OVER (ORDER BY year DESC)
            ELSE tax_park_district_data_year END AS tax_park_district_data_year,
        CASE
            WHEN tax_sanitation_district_data_year IS NULL THEN
                LAST_VALUE(tax_sanitation_district_data_year) IGNORE NULLS
                OVER (ORDER BY year DESC)
            ELSE tax_sanitation_district_data_year END AS tax_sanitation_district_data_year,
        CASE
            WHEN tax_special_service_area_data_year IS NULL THEN
                LAST_VALUE(tax_special_service_area_data_year) IGNORE NULLS
                OVER (ORDER BY year DESC)
            ELSE tax_special_service_area_data_year END AS tax_special_service_area_data_year,
        CASE
            WHEN tax_tif_district_data_year IS NULL THEN
                LAST_VALUE(tax_tif_district_data_year) IGNORE NULLS
                OVER (ORDER BY year DESC)
            ELSE tax_tif_district_data_year END AS tax_tif_district_data_year,
        CASE
            WHEN access_cmap_walk_data_year IS NULL THEN
                LAST_VALUE(access_cmap_walk_data_year) IGNORE NULLS
                OVER (ORDER BY year DESC)
            ELSE access_cmap_walk_data_year END AS access_cmap_walk_data_year,
        CASE
            WHEN misc_subdivision_data_year IS NULL THEN
                LAST_VALUE(misc_subdivision_data_year) IGNORE NULLS
                OVER (ORDER BY year DESC)
            ELSE misc_subdivision_data_year END AS misc_subdivision_data_year

    FROM unfilled
    ORDER BY YEAR
    )