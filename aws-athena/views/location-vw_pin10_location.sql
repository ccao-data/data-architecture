-- View containing each of the PIN-level location (spatial joins)
CREATE OR REPLACE VIEW location.vw_pin10_location AS
SELECT
    pin.pin10,
    pin.year,
    census_block_group_geoid,
    census_block_geoid,
    census_congressional_district_geoid,
    census_county_subdivision_geoid,
    census_place_geoid,
    census_puma_geoid,
    census_school_district_elementary_geoid,
    census_school_district_secondary_geoid,
    census_school_district_unified_geoid,
    census_state_representative_geoid,
    census_state_senate_geoid,
    census_tract_geoid,
    census_zcta_geoid,
    census_data_year,
    census_acs5_congressional_district_geoid,
    census_acs5_county_subdivision_geoid,
    census_acs5_place_geoid,
    census_acs5_puma_geoid,
    census_acs5_school_district_elementary_geoid,
    census_acs5_school_district_secondary_geoid,
    census_acs5_school_district_unified_geoid,
    census_acs5_state_representative_geoid,
    census_acs5_state_senate_geoid,
    census_acs5_tract_geoid,
    census_acs5_data_year,
    cook_board_of_review_district_num,
    cook_board_of_review_district_data_year,
    cook_commissioner_district_num,
    cook_commissioner_district_data_year,
    cook_judicial_district_num,
    cook_judicial_district_data_year,
    cook_municipality_num,
    cook_municipality_name,
    cook_municipality_data_year,
    CASE
        WHEN chicago_ward_num IS NOT NULL THEN chicago_ward_num
        WHEN evanston_ward_num IS NOT NULL THEN evanston_ward_num
        ELSE NULL END AS ward_num,
    chicago_ward_data_year,
    chicago_community_area_num,
    chicago_community_area_name,
    chicago_community_area_data_year,
    chicago_industrial_corridor_num,
    chicago_industrial_corridor_name,
    chicago_industrial_corridor_data_year,
    chicago_police_district_num,
    chicago_police_district_data_year,
    econ_coordinated_care_area_num,
    econ_coordinated_care_area_data_year,
    econ_enterprise_zone_num,
    econ_enterprise_zone_data_year,
    econ_industrial_growth_zone_num,
    econ_industrial_growth_zone_data_year,
    econ_qualified_opportunity_zone_num,
    econ_qualified_opportunity_zone_data_year,
    env_flood_fema_sfha,
    env_flood_fema_data_year,
    env_flood_fs_factor,
    env_flood_fs_risk_direction,
    env_flood_fs_data_year,
    env_ohare_noise_contour_no_buffer_bool,
    env_ohare_noise_contour_half_mile_buffer_bool,
    env_ohare_noise_contour_data_year,
    env_airport_noise_dnl,
    env_airport_noise_data_year,
    school_elementary_district_geoid,
    school_elementary_district_name,
    school_secondary_district_geoid,
    school_secondary_district_name,
    school_unified_district_geoid,
    school_unified_district_name,
    school_school_year,
    school_data_year,
    tax_community_college_district_num,
    tax_community_college_district_name,
    tax_community_college_district_data_year,
    tax_fire_protection_district_num,
    tax_fire_protection_district_name,
    tax_fire_protection_district_data_year,
    tax_library_district_num,
    tax_library_district_name,
    tax_library_district_data_year,
    tax_park_district_num,
    tax_park_district_name,
    tax_park_district_data_year,
    tax_sanitation_district_num,
    tax_sanitation_district_name,
    tax_sanitation_district_data_year,
    tax_special_service_area_num,
    tax_special_service_area_name,
    tax_special_service_area_data_year,
    tax_tif_district_num,
    tax_tif_district_name,
    tax_tif_district_data_year,
    access_cmap_walk_id,
    access_cmap_walk_nta_score,
    access_cmap_walk_total_score,
    access_cmap_walk_data_year,
    misc_subdivision_id,
    misc_subdivision_data_year,
    misc_unincorporated_area_bool,
    misc_unincorporated_area_data_year
FROM spatial.parcel pin
LEFT JOIN location.census
    ON pin.pin10 = census.pin10
    AND pin.year = census.year
LEFT JOIN location.census_acs5
    ON pin.pin10 = census_acs5.pin10
    AND pin.year = census_acs5.year
LEFT JOIN location.political
    ON pin.pin10 = political.pin10
    AND pin.year = political.year
LEFT JOIN location.chicago
    ON pin.pin10 = chicago.pin10
    AND pin.year = chicago.year
LEFT JOIN location.evanston
    ON pin.pin10 = evanston.pin10
    AND pin.year = evanston.year
LEFT JOIN location.economy
    ON pin.pin10 = economy.pin10
    AND pin.year = economy.year
LEFT JOIN location.environment
    ON pin.pin10 = environment.pin10
    AND pin.year = environment.year
LEFT JOIN location.school
    ON pin.pin10 = school.pin10
    AND pin.year = school.year
LEFT JOIN location.tax
    ON pin.pin10 = tax.pin10
    AND pin.year = tax.year
LEFT JOIN location.access
    ON pin.pin10 = access.pin10
    AND pin.year = access.year
LEFT JOIN location.other
    ON pin.pin10 = other.pin10
    AND pin.year = other.year