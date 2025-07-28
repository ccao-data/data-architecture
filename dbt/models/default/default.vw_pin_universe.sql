-- Source of truth view for PIN location

/* This CTE ensures the most recent year of pardat data is joined to spatial
data regardless of timing. This is necessary since spatial data releases late in
a calendar year while the CCAO's universe of PINs is rolled over much sooner.

Specifically, the CCAO usually rolls over iasWorld in January of each year while
spatial data (parcel, tiger/line shapefiles, etc.) do not typically become
available until November or Decemeber of that same year. This leaves almost an
entire calendar year duing which the most recent (and relevant) year of iasWorld
data is unmatched with spatial data. */
WITH pardat_adjusted_years AS (

    SELECT
        parid,
        taxyr,
        CASE
            WHEN
                taxyr
                > (SELECT MAX(year) FROM {{ source('spatial', 'parcel') }})
                THEN (SELECT MAX(year) FROM {{ source('spatial', 'parcel') }})
            ELSE taxyr
        END AS join_year,
        class,
        nbhd,
        cur,
        deactivat
    FROM {{ source('iasworld', 'pardat') }}

)

SELECT
    -- Main PIN-level attribute data from iasWorld
    par.parid AS pin,
    SUBSTR(par.parid, 1, 10) AS pin10,
    par.taxyr AS year,
    REGEXP_REPLACE(par.class, '[^[:alnum:]]', '') AS class,
    twn.triad_name,
    twn.triad_code,
    twn.township_name,
    leg.user1 AS township_code,
    REGEXP_REPLACE(par.nbhd, '([^0-9])', '') AS nbhd_code,
    leg.taxdist AS tax_code,
    NULLIF(leg.zip1, '00000') AS zip_code,

    -- Centroid of each PIN from county parcel files
    sp.lon,
    sp.lat,
    sp.x_3435,
    sp.y_3435,

    -- PIN locations from spatial joins
    vwl.census_block_group_geoid,
    vwl.census_block_geoid,
    vwl.census_congressional_district_geoid,
    SUBSTR(vwl.census_congressional_district_geoid, 3, 2)
        AS census_congressional_district_num,
    vwl.census_county_subdivision_geoid,
    vwl.census_place_geoid,
    vwl.census_puma_geoid,
    vwl.census_school_district_elementary_geoid,
    vwl.census_school_district_secondary_geoid,
    vwl.census_school_district_unified_geoid,
    vwl.census_state_representative_geoid,
    SUBSTR(vwl.census_state_representative_geoid, 4, 2)
        AS census_state_representative_num,
    vwl.census_state_senate_geoid,
    SUBSTR(vwl.census_state_senate_geoid, 4, 2) AS census_state_senate_num,
    vwl.census_tract_geoid,
    vwl.census_zcta_geoid,
    vwl.census_data_year,
    vwl.census_acs5_congressional_district_geoid,
    SUBSTR(vwl.census_acs5_congressional_district_geoid, 3, 2)
        AS census_acs5_congressional_district_num,
    vwl.census_acs5_county_subdivision_geoid,
    vwl.census_acs5_place_geoid,
    vwl.census_acs5_puma_geoid,
    vwl.census_acs5_school_district_elementary_geoid,
    vwl.census_acs5_school_district_secondary_geoid,
    vwl.census_acs5_school_district_unified_geoid,
    vwl.census_acs5_state_representative_geoid,
    SUBSTR(vwl.census_acs5_state_representative_geoid, 4, 2)
        AS census_acs5_state_representative_num,
    vwl.census_acs5_state_senate_geoid,
    SUBSTR(vwl.census_acs5_state_senate_geoid, 4, 2)
        AS census_acs5_state_senate_num,
    vwl.census_acs5_tract_geoid,
    vwl.census_acs5_data_year,
    vwl.cook_board_of_review_district_num,
    vwl.cook_board_of_review_district_data_year,
    vwl.cook_commissioner_district_num,
    vwl.cook_commissioner_district_data_year,
    vwl.cook_judicial_district_num,
    vwl.cook_judicial_district_data_year,
    vwl.cook_municipality_name,
    vwl.cook_municipality_num,
    vwl.cook_municipality_data_year,
    vwl.ward_num,
    vwl.ward_name,
    vwl.ward_chicago_data_year,
    vwl.ward_evanston_data_year,
    vwl.chicago_community_area_num,
    vwl.chicago_community_area_name,
    vwl.chicago_community_area_data_year,
    vwl.chicago_industrial_corridor_num,
    vwl.chicago_industrial_corridor_name,
    vwl.chicago_industrial_corridor_data_year,
    vwl.chicago_police_district_num,
    vwl.chicago_police_district_data_year,
    vwl.econ_coordinated_care_area_num,
    vwl.econ_coordinated_care_area_data_year,
    vwl.econ_enterprise_zone_num,
    vwl.econ_enterprise_zone_data_year,
    vwl.econ_industrial_growth_zone_num,
    vwl.econ_industrial_growth_zone_data_year,
    vwl.econ_qualified_opportunity_zone_num,
    vwl.econ_qualified_opportunity_zone_data_year,
    vwl.econ_central_business_district_num,
    vwl.econ_central_business_district_data_year,
    vwl.env_flood_fema_sfha,
    vwl.env_flood_fema_data_year,
    vwl.env_flood_fs_factor,
    vwl.env_flood_fs_risk_direction,
    vwl.env_flood_fs_data_year,
    vwl.env_ohare_noise_contour_no_buffer_bool,
    vwl.env_ohare_noise_contour_half_mile_buffer_bool,
    vwl.env_ohare_noise_contour_data_year,
    vwl.env_airport_noise_dnl,
    vwl.env_airport_noise_data_year,
    vwl.school_elementary_district_geoid,
    vwl.school_elementary_district_name,
    vwl.school_secondary_district_geoid,
    vwl.school_secondary_district_name,
    vwl.school_unified_district_geoid,
    vwl.school_unified_district_name,
    vwl.school_school_year,
    vwl.school_data_year,
    vwl.tax_municipality_num,
    vwl.tax_municipality_name,
    vwl.tax_school_elementary_district_num,
    vwl.tax_school_elementary_district_name,
    vwl.tax_school_secondary_district_num,
    vwl.tax_school_secondary_district_name,
    vwl.tax_school_unified_district_num,
    vwl.tax_school_unified_district_name,
    vwl.tax_community_college_district_num,
    vwl.tax_community_college_district_name,
    vwl.tax_fire_protection_district_num,
    vwl.tax_fire_protection_district_name,
    vwl.tax_library_district_num,
    vwl.tax_library_district_name,
    vwl.tax_park_district_num,
    vwl.tax_park_district_name,
    vwl.tax_sanitation_district_num,
    vwl.tax_sanitation_district_name,
    vwl.tax_special_service_area_num,
    vwl.tax_special_service_area_name,
    vwl.tax_tif_district_num,
    vwl.tax_tif_district_name,
    vwl.tax_data_year,
    vwl.access_cmap_walk_id,
    vwl.access_cmap_walk_nta_score,
    vwl.access_cmap_walk_total_score,
    vwl.access_cmap_walk_data_year,
    vwl.misc_subdivision_id,
    vwl.misc_subdivision_data_year
FROM pardat_adjusted_years AS par
LEFT JOIN {{ source('iasworld', 'legdat') }} AS leg
    ON par.parid = leg.parid
    AND par.taxyr = leg.taxyr
    AND leg.cur = 'Y'
    AND leg.deactivat IS NULL
LEFT JOIN {{ source('spatial', 'parcel') }} AS sp
    ON SUBSTR(par.parid, 1, 10) = sp.pin10
    AND par.join_year = sp.year
LEFT JOIN {{ ref('location.vw_pin10_location') }} AS vwl
    ON SUBSTR(par.parid, 1, 10) = vwl.pin10
    AND par.join_year = vwl.year
LEFT JOIN {{ source('spatial', 'township') }} AS twn
    ON leg.user1 = CAST(twn.township_code AS VARCHAR)
WHERE par.cur = 'Y'
    AND par.deactivat IS NULL
    -- Remove any parcels with non-numeric characters
    -- or that are not 14 characters long
    AND REGEXP_COUNT(par.parid, '[a-zA-Z]') = 0
    AND LENGTH(par.parid) = 14
    -- Class 999 are test pins
    AND par.class NOT IN ('999')
