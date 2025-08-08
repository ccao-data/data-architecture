-- View containing each of the PIN-level location (spatial joins)

-- Grab PINs that have been created more recently than the latest year in the
-- tax.pin table
WITH new_pins AS (
    SELECT
        SUBSTR(parid, 1, 10) AS pin10,
        MIN(taxyr) AS year
    FROM {{ source('iasworld', 'pardat') }}
    WHERE cur = 'Y'
        AND deactivat IS NULL
    GROUP BY SUBSTR(parid, 1, 10)
    HAVING MIN(taxyr) > (SELECT MAX(year) FROM tax.pin)
)

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

    political.cook_board_of_review_district_num,
    political.cook_board_of_review_district_data_year,
    political.cook_commissioner_district_num,
    political.cook_commissioner_district_data_year,
    political.cook_judicial_district_num,
    political.cook_judicial_district_data_year,
    political.cook_municipality_num,
    political.cook_municipality_name,
    political.cook_municipality_data_year,
    political.ward_num,
    political.ward_name,
    political.ward_chicago_data_year,
    political.ward_evanston_data_year,

    chicago.chicago_community_area_num,
    chicago.chicago_community_area_name,
    chicago.chicago_community_area_data_year,
    chicago.chicago_industrial_corridor_num,
    chicago.chicago_industrial_corridor_name,
    chicago.chicago_industrial_corridor_data_year,
    chicago.chicago_police_district_num,
    chicago.chicago_police_district_data_year,

    economy.econ_coordinated_care_area_num,
    economy.econ_coordinated_care_area_data_year,
    economy.econ_enterprise_zone_num,
    economy.econ_enterprise_zone_data_year,
    economy.econ_industrial_growth_zone_num,
    economy.econ_industrial_growth_zone_data_year,
    economy.econ_qualified_opportunity_zone_num,
    economy.econ_qualified_opportunity_zone_data_year,
    economy.econ_central_business_district_num,
    economy.econ_central_business_district_data_year,

    environment.env_flood_fema_sfha,
    environment.env_flood_fema_data_year,
    environment.env_flood_fs_factor,
    environment.env_flood_fs_risk_direction,
    environment.env_flood_fs_data_year,
    environment.env_ohare_noise_contour_no_buffer_bool,
    environment.env_ohare_noise_contour_half_mile_buffer_bool,
    environment.env_ohare_noise_contour_data_year,
    environment.env_airport_noise_dnl,
    environment.env_airport_noise_data_year,

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
    -- PINs created after the most recent year of tax data won't have values for
    -- tax_municipality_name.
    CASE
        WHEN new_pins.pin10 IS NOT NULL
            AND political.cook_municipality_name[1] NOT IN ('UNINCORPORATED')
            THEN ARRAY[
                COALESCE(
                    xwalk.tax_municipality_name,
                    political.cook_municipality_name[1]
                )
            ]
        ELSE tax.tax_municipality_name
    END AS combined_municipality_name,
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
LEFT JOIN {{ ref('location.census') }} AS census
    ON pin.pin10 = census.pin10
    AND pin.year = census.year
LEFT JOIN {{ ref('location.census_acs5') }} AS census_acs5
    ON pin.pin10 = census_acs5.pin10
    AND pin.year = census_acs5.year
LEFT JOIN {{ ref('location.political') }} AS political
    ON pin.pin10 = political.pin10
    AND pin.year = political.year
LEFT JOIN {{ ref('location.municipality_crosswalk') }} AS xwalk
    ON political.cook_municipality_name[1] = xwalk.cook_municipality_name
LEFT JOIN {{ ref('location.chicago') }} AS chicago
    ON pin.pin10 = chicago.pin10
    AND pin.year = chicago.year
LEFT JOIN {{ ref('location.economy') }} AS economy
    ON pin.pin10 = economy.pin10
    AND pin.year = economy.year
LEFT JOIN {{ ref('location.environment') }} AS environment
    ON pin.pin10 = environment.pin10
    AND pin.year = environment.year
LEFT JOIN {{ ref('location.school') }} AS school
    ON pin.pin10 = school.pin10
    AND pin.year = school.year
LEFT JOIN {{ ref('location.tax') }} AS tax
    ON pin.pin10 = tax.pin10
    AND pin.year = tax.year
LEFT JOIN {{ ref('location.access') }} AS access
    ON pin.pin10 = access.pin10
    AND pin.year = access.year
LEFT JOIN {{ ref('location.other') }} AS other
    ON pin.pin10 = other.pin10
    AND pin.year = other.year
LEFT JOIN
    new_pins ON pin.pin10 = new_pins.pin10
