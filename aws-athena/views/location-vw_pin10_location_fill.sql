-- View containing each of the PIN-level location (spatial joins)
SELECT
    pin.pin10,
    pin.year,

    census.census_block_group_geoid,
    census.census_block_geoid,
    census.census_congressional_district_geoid,
    census.census_county_subdivision_geoid,
    census.census_place_geoid,
    census.census_puma_geoid,
    census.census_school_district_elementary_geoid,
    census.census_school_district_secondary_geoid,
    census.census_school_district_unified_geoid,
    census.census_state_representative_geoid,
    census.census_state_senate_geoid,
    census.census_tract_geoid,
    census.census_zcta_geoid,
    census.census_data_year,

    census_acs5.census_acs5_congressional_district_geoid,
    census_acs5.census_acs5_county_subdivision_geoid,
    census_acs5.census_acs5_place_geoid,
    census_acs5.census_acs5_puma_geoid,
    census_acs5.census_acs5_school_district_elementary_geoid,
    census_acs5.census_acs5_school_district_secondary_geoid,
    census_acs5.census_acs5_school_district_unified_geoid,
    census_acs5.census_acs5_state_representative_geoid,
    census_acs5.census_acs5_state_senate_geoid,
    census_acs5.census_acs5_tract_geoid,
    census_acs5.census_acs5_data_year,

    cook_board_of_review_district.cook_board_of_review_district_num,
    cook_board_of_review_district.cook_board_of_review_district_data_year,
    cook_commissioner_district.cook_commissioner_district_num,
    cook_commissioner_district.cook_commissioner_district_data_year,
    cook_judicial_district.cook_judicial_district_num,
    cook_judicial_district.cook_judicial_district_data_year,
    COALESCE(ward_evanston.ward_num, ward_chicago.ward_num) AS ward_num,
    COALESCE(ward_evanston.ward_name, ward_chicago.ward_name) AS ward_name,
    ward_chicago.ward_chicago_data_year,
    ward_evanston.ward_evanston_data_year,

    chicago_community_area.chicago_community_area_num,
    chicago_community_area.chicago_community_area_name,
    chicago_community_area.chicago_community_area_data_year,
    chicago_industrial_corridor.chicago_industrial_corridor_num,
    chicago_industrial_corridor.chicago_industrial_corridor_name,
    chicago_industrial_corridor.chicago_industrial_corridor_data_year,
    chicago_police_district.chicago_police_district_num,
    chicago_police_district.chicago_police_district_data_year,

    econ_coordinated_care_area.econ_coordinated_care_area_num,
    econ_coordinated_care_area.econ_coordinated_care_area_data_year,
    econ_enterprise_zone.econ_enterprise_zone_num,
    econ_enterprise_zone.econ_enterprise_zone_data_year,
    econ_industrial_growth_zone.econ_industrial_growth_zone_num,
    econ_industrial_growth_zone.econ_industrial_growth_zone_data_year,
    econ_qualified_opportunity_zone.econ_qualified_opportunity_zone_num,
    econ_qualified_opportunity_zone.econ_qualified_opportunity_zone_data_year,

    env_flood_fema.env_flood_fema_sfha,
    env_flood_fema.env_flood_fema_data_year,
    env_flood_fs.env_flood_fs_factor,
    env_flood_fs.env_flood_fs_risk_direction,
    env_flood_fs.env_flood_fs_data_year,
    env_ohare_noise_contour.env_ohare_noise_contour_no_buffer_bool,
    env_ohare_noise_contour.env_ohare_noise_contour_half_mile_buffer_bool,
    env_ohare_noise_contour.env_ohare_noise_contour_data_year,
    env_airport_noise.env_airport_noise_dnl,
    env_airport_noise.env_airport_noise_data_year,

    school.school_elementary_district_geoid,
    school.school_elementary_district_name,
    school.school_secondary_district_geoid,
    school.school_secondary_district_name,
    school.school_unified_district_geoid,
    school.school_unified_district_name,
    school.school_school_year,
    school.school_data_year,

    tax.tax_municipality_num,
    tax.tax_municipality_name,
    tax.tax_school_elementary_district_num,
    tax.tax_school_elementary_district_name,
    tax.tax_school_secondary_district_num,
    tax.tax_school_secondary_district_name,
    tax.tax_school_unified_district_num,
    tax.tax_school_unified_district_name,
    tax.tax_community_college_district_num,
    tax.tax_community_college_district_name,
    tax.tax_fire_protection_district_num,
    tax.tax_fire_protection_district_name,
    tax.tax_library_district_num,
    tax.tax_library_district_name,
    tax.tax_park_district_num,
    tax.tax_park_district_name,
    tax.tax_sanitation_district_num,
    tax.tax_sanitation_district_name,
    tax.tax_special_service_area_num,
    tax.tax_special_service_area_name,
    tax.tax_tif_district_num,
    tax.tax_tif_district_name,
    tax.tax_data_year,

    access.access_cmap_walk_id,
    access.access_cmap_walk_nta_score,
    access.access_cmap_walk_total_score,
    access.access_cmap_walk_data_year,

    other.misc_subdivision_id,
    other.misc_subdivision_data_year
FROM {{ source('spatial', 'parcel') }} AS pin
INNER JOIN {{ ref('location.crosswalk_year_fill') }} AS cyf
    ON pin.year = cyf.year
LEFT JOIN {{ ref('location.census') }} AS census
    ON pin.pin10 = census.pin10
    AND cyf.census_data_year = census.year
LEFT JOIN {{ ref('location.census_acs5') }} AS census_acs5
    ON pin.pin10 = census_acs5.pin10
    AND cyf.census_acs5_data_year = census_acs5.year
LEFT JOIN {{ ref('location.political') }} AS cook_board_of_review_district
    ON pin.pin10 = cook_board_of_review_district.pin10
    AND cyf.cook_board_of_review_district_data_year
    = cook_board_of_review_district.year
LEFT JOIN {{ ref('location.political') }} AS cook_commissioner_district
    ON pin.pin10 = cook_commissioner_district.pin10
    AND cyf.cook_commissioner_district_data_year
    = cook_commissioner_district.year
LEFT JOIN {{ ref('location.political') }} AS cook_judicial_district
    ON pin.pin10 = cook_judicial_district.pin10
    AND cyf.cook_judicial_district_data_year = cook_judicial_district.year
LEFT JOIN {{ ref('location.political') }} AS ward_chicago
    ON pin.pin10 = ward_chicago.pin10
    AND cyf.ward_chicago_data_year = ward_chicago.year
LEFT JOIN {{ ref('location.political') }} AS ward_evanston
    ON pin.pin10 = ward_evanston.pin10
    AND cyf.ward_evanston_data_year = ward_evanston.year
LEFT JOIN {{ ref('location.chicago') }} AS chicago_community_area
    ON pin.pin10 = chicago_community_area.pin10
    AND cyf.chicago_community_area_data_year = chicago_community_area.year
LEFT JOIN {{ ref('location.chicago') }} AS chicago_industrial_corridor
    ON pin.pin10 = chicago_industrial_corridor.pin10
    AND cyf.chicago_industrial_corridor_data_year
    = chicago_industrial_corridor.year
LEFT JOIN {{ ref('location.chicago') }} AS chicago_police_district
    ON pin.pin10 = chicago_police_district.pin10
    AND cyf.chicago_police_district_data_year = chicago_police_district.year
LEFT JOIN {{ ref('location.economy') }} AS econ_coordinated_care_area
    ON pin.pin10 = econ_coordinated_care_area.pin10
    AND cyf.econ_coordinated_care_area_data_year
    = econ_coordinated_care_area.year
LEFT JOIN {{ ref('location.economy') }} AS econ_enterprise_zone
    ON pin.pin10 = econ_enterprise_zone.pin10
    AND cyf.econ_enterprise_zone_data_year = econ_enterprise_zone.year
LEFT JOIN {{ ref('location.economy') }} AS econ_industrial_growth_zone
    ON pin.pin10 = econ_industrial_growth_zone.pin10
    AND cyf.econ_industrial_growth_zone_data_year
    = econ_industrial_growth_zone.year
LEFT JOIN {{ ref('location.economy') }} AS econ_qualified_opportunity_zone
    ON pin.pin10 = econ_qualified_opportunity_zone.pin10
    AND cyf.econ_qualified_opportunity_zone_data_year
    = econ_qualified_opportunity_zone.year
LEFT JOIN {{ ref('location.environment') }} AS env_flood_fema
    ON pin.pin10 = env_flood_fema.pin10
    AND cyf.env_flood_fema_data_year = env_flood_fema.year
LEFT JOIN {{ ref('location.environment') }} AS env_flood_fs
    ON pin.pin10 = env_flood_fs.pin10
    AND cyf.env_flood_fs_data_year = env_flood_fs.year
LEFT JOIN {{ ref('location.environment') }} AS env_ohare_noise_contour
    ON pin.pin10 = env_ohare_noise_contour.pin10
    AND cyf.env_ohare_noise_contour_data_year = env_ohare_noise_contour.year
-- Airport noise is joined differently since it's filled during ingest and
-- values for env_airport_noise_data_year won't match values for year
LEFT JOIN {{ ref('location.environment') }} AS env_airport_noise
    ON pin.pin10 = env_airport_noise.pin10
    AND ((
        cyf.env_airport_noise_data_year = env_airport_noise.year
        AND cyf.env_airport_noise_data_year != 'opm'
    )
    OR (
        cyf.year = env_airport_noise.year
        AND cyf.env_airport_noise_data_year = 'opm'
    ))
LEFT JOIN {{ ref('location.school') }} AS school
    ON pin.pin10 = school.pin10
    AND cyf.school_data_year = school.year
LEFT JOIN {{ ref('location.tax') }} AS tax
    ON pin.pin10 = tax.pin10
    AND cyf.tax_data_year = tax.year
LEFT JOIN {{ ref('location.access') }} AS access
    ON pin.pin10 = access.pin10
    AND cyf.access_cmap_walk_data_year = access.year
LEFT JOIN {{ ref('location.other') }} AS other
    ON pin.pin10 = other.pin10
    AND cyf.misc_subdivision_data_year = other.year
