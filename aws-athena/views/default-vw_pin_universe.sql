-- Source of truth view for PIN location, address, and distances
CREATE OR REPLACE VIEW default.vw_pin_universe AS
SELECT
    -- Main PIN-level attribute data from iasWorld
    par.parid AS pin,
    SUBSTR(par.parid, 1, 10) AS pin10,
    par.taxyr AS year,
    regexp_replace(par.class,'([^0-9EXR])','') AS class,
    twn.triad_name,
    twn.triad_code,
    twn.township_name,
    SUBSTR(leg.taxdist, 1, 2) AS township_code,
    regexp_replace(par.nbhd,'([^0-9])','') AS nbhd_code,
    leg.taxdist AS tax_code,

    -- Proration related fields from PARDAT
    par.tieback AS tieback_key_pin,
    CASE
        WHEN par.tiebldgpct IS NOT NULL THEN par.tiebldgpct / 100.0
        WHEN par.tiebldgpct IS NULL AND par.class IN ('299', '399') THEN 0
        ELSE 1.0
    END AS tieback_proration_rate,

    -- Centroid of each PIN from county parcel files
    sp.lon, sp.lat, sp.x_3435, sp.y_3435,

    -- PIN legal address from LEGDAT
    leg.adrpre AS prop_address_prefix,
    leg.adrno AS prop_address_street_number,
    leg.adrdir AS prop_address_street_dir,
    leg.adrstr AS prop_address_street_name,
    leg.adrsuf AS prop_address_suffix_1,
    leg.adrsuf2 AS prop_address_suffix_2,
    leg.unitdesc AS prop_address_unit_prefix,
    leg.unitno AS prop_address_unit_number,
    NULLIF(CONCAT_WS(
        ' ',
        leg.adrpre, CAST(leg.adrno AS varchar),
        leg.adrdir, leg.adrstr, leg.adrsuf,
        leg.unitdesc, leg.unitno
    ), '') AS prop_address_full,
    leg.cityname AS prop_address_city_name,
    leg.statecode AS prop_address_state,
    NULLIF(leg.zip1, '00000') AS prop_address_zipcode_1,
    NULLIF(leg.zip2, '0000') AS prop_address_zipcode_2,

    -- PIN mailing address from OWNDAT
    NULLIF(CONCAT_WS(
        ' ',
        own.own1, own.own2
    ), '') AS mail_address_name,
    CASE WHEN NULLIF(own.addr1, '') IS NOT NULL THEN own.addr1
         WHEN NULLIF(own.addr2, '') IS NOT NULL THEN own.addr2
         ELSE NULLIF(CONCAT_WS(' ',
             CAST(own.adrno AS varchar),
             own.adrdir, own.adrstr, own.adrsuf,
             own.unitdesc, own.unitno
         ), '')
    END AS mail_address_full,
    own.cityname AS mail_address_city_name,
    own.statecode AS mail_address_state,
    NULLIF(own.zip1, '00000') AS mail_address_zipcode_1,
    NULLIF(own.zip2, '0000') AS mail_address_zipcode_2,

    -- PIN locations from spatial joins
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
    chicago_ward_num,
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
    misc_unincorporated_area_data_year,

    -- PIN proximity measurements from CTAS
    num_pin_in_half_mile,
    num_bus_stop_in_half_mile,
    num_bus_stop_data_year,
    num_foreclosure_in_half_mile_past_5_years,
    num_foreclosure_per_1000_pin_past_5_years,
    num_foreclosure_data_year,
    num_school_in_half_mile,
    num_school_with_rating_in_half_mile,
    avg_school_rating_in_half_mile,
    num_school_data_year,
    num_school_rating_data_year,
    nearest_bike_trail_id,
    nearest_bike_trail_name,
    nearest_bike_trail_dist_ft,
    nearest_bike_trail_data_year,
    nearest_cemetery_gnis_code,
    nearest_cemetery_name,
    nearest_cemetery_dist_ft,
    nearest_cemetery_data_year,
    nearest_cta_route_id,
    nearest_cta_route_name,
    nearest_cta_route_dist_ft,
    nearest_cta_route_data_year,
    nearest_cta_stop_id,
    nearest_cta_stop_name,
    nearest_cta_stop_dist_ft,
    nearest_cta_stop_data_year,
    nearest_hospital_gnis_code,
    nearest_hospital_name,
    nearest_hospital_dist_ft,
    nearest_hospital_data_year,
    lake_michigan_dist_ft,
    lake_michigan_data_year,
    nearest_major_road_osm_id,
    nearest_major_road_name,
    nearest_major_road_dist_ft,
    nearest_major_road_data_year,
    nearest_metra_route_id,
    nearest_metra_route_name,
    nearest_metra_route_dist_ft,
    nearest_metra_route_data_year,
    nearest_metra_stop_id,
    nearest_metra_stop_name,
    nearest_metra_stop_dist_ft,
    nearest_metra_stop_data_year,
    nearest_park_osm_id,
    nearest_park_name,
    nearest_park_dist_ft,
    nearest_park_data_year,
    nearest_railroad_id,
    nearest_railroad_name,
    nearest_railroad_dist_ft,
    nearest_railroad_data_year,
    nearest_water_id,
    nearest_water_name,
    nearest_water_dist_ft,
    nearest_water_data_year,

    -- Distance to nearest neighbors
    nearest_neighbor_1_pin10,
    nearest_neighbor_1_dist_ft,
    nearest_neighbor_2_pin10,
    nearest_neighbor_2_dist_ft,
    nearest_neighbor_3_pin10,
    nearest_neighbor_3_dist_ft
FROM iasworld.pardat par
LEFT JOIN iasworld.legdat leg
    ON par.parid = leg.parid
    AND par.taxyr = leg.taxyr
LEFT JOIN iasworld.owndat own
    ON par.parid = own.parid
    AND par.taxyr = own.taxyr
LEFT JOIN spatial.parcel sp
    ON SUBSTR(par.parid, 1, 10) = sp.pin10
    AND par.taxyr = sp.year
LEFT JOIN location.vw_pin10_location vwl
    ON SUBSTR(par.parid, 1, 10) = vwl.pin10
    AND par.taxyr = vwl.year
LEFT JOIN proximity.vw_pin10_proximity vwp
    ON SUBSTR(par.parid, 1, 10) = vwp.pin10
    AND par.taxyr = vwp.year
LEFT JOIN spatial.township twn
    ON SUBSTR(leg.taxdist, 1, 2) = CAST(twn.township_code AS varchar)