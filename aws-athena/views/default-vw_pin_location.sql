-- View containing each of the PIN-level location (spatial joins)
CREATE OR REPLACE VIEW default.vw_pin_location AS
SELECT
    pin.pin10,
    pin.year,
    census_geoid_block_group,
    census_geoid_block,
    census_geoid_congressional_district,
    census_geoid_county_subdivision,
    census_geoid_place,
    census_geoid_puma,
    census_geoid_school_district_elementary,
    census_geoid_school_district_secondary,
    census_geoid_school_district_unified,
    census_geoid_state_representative,
    census_geoid_state_senate,
    census_geoid_tract,
    census_geoid_zcta,
    cook_dist_num_board_of_review,
    cook_dist_num_county_commissioner,
    cook_dist_num_judicial,
    cook_municipality_name,
    chicago_num_ward,
    chicago_num_community_area,
    chicago_name_community_area,
    chicago_num_industrial_corridor,
    chicago_name_industrial_corridor,
    chicago_num_police_district,
    econ_zone_consolidated_care,
    econ_zone_enterprise,
    econ_zone_industrial_growth,
    econ_zone_qualified_opportunity,
    env_flood_fema_sfha,
    env_flood_fs_factor,
    env_flood_fs_risk_direction,
    env_ohare_noise_contour_no_buffer,
    env_ohare_noise_contour_half_mil_buffer,
    school_dist_geoid_elementary,
    school_dist_name_elementary,
    school_dist_geoid_secondary,
    school_dist_name_secondary,
    school_dist_geoid_unified,
    school_dist_name_unified,
    tax_dist_num_community_college,
    tax_dist_name_community_college,
    tax_dist_num_fire_protection,
    tax_dist_name_fire_protection,
    tax_dist_num_library,
    tax_dist_name_library,
    tax_dist_num_park,
    tax_dist_name_park,
    tax_dist_num_sanitation,
    tax_dist_name_sanitation,
    tax_dist_num_ssa,
    tax_dist_name_ssa,
    tax_dist_num_tif,
    tax_dist_name_tif,
    misc_subdivision_id,
    misc_unincorporated_area
FROM spatial.parcel pin
LEFT JOIN location.census
    ON pin.pin10 = census.pin10
    AND pin.year = census.year
LEFT JOIN location.political
    ON pin.pin10 = political.pin10
    AND pin.year = political.year
LEFT JOIN location.chicago
    ON pin.pin10 = chicago.pin10
    AND pin.year = chicago.year
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
LEFT JOIN location.other
    ON pin.pin10 = other.pin10
    AND pin.year = other.year
WHERE pin.year >= '2013'