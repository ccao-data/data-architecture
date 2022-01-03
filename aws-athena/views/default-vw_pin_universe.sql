-- Source of truth view for PIN location, address, and distances
CREATE OR REPLACE VIEW default.vw_pin_universe AS
SELECT
    -- Main PIN-level attribute data from iasWorld
    par.parid AS pin,
    SUBSTR(par.parid, 1, 10) AS pin10,
    par.taxyr AS year,
    par.class AS class,
    SUBSTR(leg.taxdist, 1, 2) AS town_code,
    par.nbhd AS nbhd_code,
    leg.taxdist AS tax_code,

    -- Centroid of each PIN from county parcel files
    sp.lon, sp.lat, sp.x_3435, sp.y_3435,

    -- PIN legal address from LEGDAT
    leg.adrpre AS address_prefix,
    leg.adrno AS address_street_number,
    leg.adrdir AS address_street_dir,
    leg.adrstr AS address_street_name,
    leg.adrsuf AS address_suffix_1,
    leg.adrsuf2 AS address_suffix_2,
    leg.unitdesc AS address_unit_prefix,
    leg.unitno AS address_unit_number,
    leg.cityname AS address_city_name,
    leg.statecode AS address_state,
    leg.zip1 AS address_zipcode_1,
    leg.zip2 AS address_zipcode_2,

    -- PIN locations from spatial joins
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
    census_geoid_state_senate
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

    -- PIN proximity measurements from CTAS
    total_num_pins_in_half_mile,
    bus_num_stops_in_half_mile,
    sch_num_schools_in_half_mile,
    sch_avg_rating_in_half_mile,
    fc_num_foreclosures_in_half_mile_past_5_years,
    fc_num_foreclosures_per_1000_props_past_5_years,
    bike_trail_gnis_code,
    bike_trail_name,
    bike_trail_dist_ft,
    cemetery_gnis_code,
    cemetery_name,
    cemetery_dist_ft,
    cta_route_id,
    cta_route_name,
    cta_route_dist_ft,
    cta_stop_id,
    cta_stop_name,
    cta_stop_dist_ft,
    hospital_gnis_code,
    hospital_name,
    hospital_dist_ft,
    lake_michigan_dist_ft,
    road_osm_id,
    road_name,
    road_dist_ft,
    metra_route_id,
    metra_route_name,
    metra_route_dist_ft,
    metra_stop_id,
    metra_stop_name,
    metra_stop_dist_ft,
    park_name,
    park_osm_id,
    park_dist_ft,
    railroad_name,
    railroad_unique_id,
    railroad_dist_ft,
    water_name,
    water_id,
    water_dist_ft
FROM iasworld.pardat par
LEFT JOIN iasworld.legdat leg
    ON par.parid = leg.parid
    AND par.taxyr = leg.taxyr
LEFT JOIN spatial.parcel sp
    ON SUBSTR(par.parid, 1, 10) = sp.pin10
    AND par.taxyr = sp.year
LEFT JOIN default.vw_pin_location vwl
    ON SUBSTR(par.parid, 1, 10) = vwl.pin10
    AND par.taxyr = vwl.year
LEFT JOIN default.vw_pin_proximity vwp
    ON SUBSTR(par.parid, 1, 10) = vwp.pin10
    AND par.taxyr = vwp.year
WHERE par.taxyr >= '2013'