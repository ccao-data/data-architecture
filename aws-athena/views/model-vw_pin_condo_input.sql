/*
View containing cleaned, filled data for residential condo modeling. Missing data is
filled with the following steps:

1. All historical data is filled FORWARD in time, i.e. data from 2020 fills
   2021 as long as the data isn't something which frequently changes
2. Current data is filled BACKWARD to account for missing historical data.
   Again, this is only for things unlikely to change

WARNING: This is a very heavy view. Don't use it for anything other than making
extracts for modeling
*/
CREATE OR REPLACE VIEW model.temp_vw_pin_condo_input AS
WITH uni AS (

    SELECT
        -- Main PIN-level attribute data from iasWorld
        par.parid AS pin,
        SUBSTR(par.parid, 1, 10) AS pin10,
        par.taxyr AS year,
        regexp_replace(par.class,'([^0-9EXR])','') AS class,
        twn.triad_name,
        twn.triad_code,
        twn.township_name,
        leg.user1 AS township_code,
        regexp_replace(par.nbhd,'([^0-9])','') AS nbhd_code,
        leg.taxdist AS tax_code,
        NULLIF(leg.zip1, '00000') AS zip_code,

        -- Centroid of each PIN from county parcel files
        sp.lon, sp.lat, sp.x_3435, sp.y_3435

    FROM iasworld.pardat par
    LEFT JOIN iasworld.legdat leg
        ON par.parid = leg.parid
        AND par.taxyr = leg.taxyr
    LEFT JOIN spatial.parcel sp
        ON SUBSTR(par.parid, 1, 10) = sp.pin10
        AND par.taxyr = sp.year
    LEFT JOIN spatial.township twn
        ON leg.user1 = CAST(twn.township_code AS varchar)

    WHERE class IN ('299', '399')

),
acs5 AS (
    SELECT *
    FROM census.vw_acs5_stat
    WHERE geography = 'tract'
),
housing_index AS (
    SELECT geoid, year, AVG(CAST(ihs_index AS double)) AS ihs_avg_year_index
    FROM other.ihs_index
    GROUP BY geoid, year
),
sqft_percentiles AS (
        SELECT
            ch.year, l.user1 AS township_code,
            CAST(approx_percentile(ch.char_land_sf, 0.95) AS int) AS char_land_sf_95_percentile
        FROM default.temp_vw_pin_condo_char ch
        LEFT JOIN iasworld.legdat l
            ON ch.pin = l.parid and ch.year = l.taxyr
        GROUP BY ch.year, l.user1
),
tax_bill_amount AS ( 
    -- Removing fill for now, since this will be pulled from PTAXSIM in the future
    SELECT
        pardat.parid AS pin,
        pardat.taxyr AS year,
        tax_bill_total AS tot_tax_amt,
        tax_code_rate AS tax_rate
    FROM iasworld.pardat
    LEFT JOIN tax.pin p
        ON pardat.parid = p.pin
        AND (
            CASE WHEN pardat.taxyr > (SELECT Max(year) FROM tax.pin)
                THEN (SELECT Max(year) FROM tax.pin)
                ELSE pardat.taxyr END = p.year
                    )
    LEFT JOIN (
        SELECT DISTINCT
            year, tax_code_num, tax_code_rate
        FROM tax.tax_code
        ) tax_code
        ON p.tax_code_num = tax_code.tax_code_num
        AND p.year = tax_code.year

    WHERE p.pin IS NOT NULL
),
school_district_ratings AS (
    SELECT
        district_geoid,
        district_type,
        AVG(rating) AS school_district_avg_rating,
        COUNT(*) AS num_schools_in_district
    FROM other.great_schools_rating
    GROUP BY district_geoid, district_type
)
SELECT
    uni.pin AS meta_pin,
    uni.pin10 AS meta_pin10,
    uni.year AS meta_year,
    uni.class AS meta_class,
    CASE
        WHEN ch.is_parking_space = TRUE
            OR ch.is_common_area = TRUE THEN 'NONLIVABLE'
        ELSE 'CONDO'
    END AS meta_modeling_group,
    uni.triad_name AS meta_triad_name,
    uni.triad_code AS meta_triad_code,
    uni.township_name AS meta_township_name,
    uni.township_code AS meta_township_code,
    uni.nbhd_code AS meta_nbhd_code,
    uni.tax_code AS meta_tax_code,

    -- Proration fields. Buildings can be split over multiple PINs, with each
    -- PIN owning a percentage of a building. For residential buildings, if
    -- a proration rate is NULL or 0, it's almost always actually 1
    ch.tieback_key_pin AS meta_tieback_key_pin,
    ch.tieback_proration_rate AS meta_tieback_proration_rate,
    CASE
        WHEN ch.tieback_proration_rate < 1.0 THEN true
        ELSE false
    END AS ind_pin_is_prorated,
    ch.card_protation_rate AS meta_card_protation_rate,

    -- Multicard/multi-landline related fields. Each PIN can have more than
    -- one improvement/card AND/OR more than one attached landline
    ch.card AS meta_card_num,
    ch.lline AS meta_lline_num,
    ch.pin_is_multilline AS ind_pin_is_multilline,
    ch.pin_num_lline AS meta_pin_num_lline,
    ch.pin_is_multiland AS ind_pin_is_multiland,
    ch.pin_num_landlines AS meta_pin_num_landlines,
    ch.cdu AS meta_cdu,

    -- PIN AV history for use in reporting and aggregate stats generation
    hist.mailed_bldg AS meta_mailed_bldg,
    hist.mailed_land AS meta_mailed_land,
    hist.mailed_tot AS meta_mailed_tot,
    hist.certified_bldg AS meta_certified_bldg,
    hist.certified_land AS meta_certified_land,
    hist.certified_tot AS meta_certified_tot,
    hist.board_bldg AS meta_board_bldg,
    hist.board_land AS meta_board_land,
    hist.board_tot AS meta_board_tot,
    hist.oneyr_pri_board_bldg AS meta_1yr_pri_board_bldg,
    hist.oneyr_pri_board_land AS meta_1yr_pri_board_land,
    hist.oneyr_pri_board_tot AS meta_1yr_pri_board_tot,
    hist.twoyr_pri_board_bldg AS meta_2yr_pri_board_bldg,
    hist.twoyr_pri_board_land AS meta_2yr_pri_board_land,
    hist.twoyr_pri_board_tot AS meta_2yr_pri_board_tot,

    -- Individual PIN-level address/location
    vwpa.prop_address_full AS loc_property_address,
    vwpa.prop_address_city_name AS loc_property_city,
    vwpa.prop_address_state AS loc_property_state,
    vwpa.prop_address_zipcode_1 AS loc_property_zip,
    uni.lon AS loc_longitude,
    uni.lat AS loc_latitude,
    uni.x_3435 AS loc_x_3435,
    uni.y_3435 AS loc_y_3435,

    -- Property characteristics from iasWorld
    ch.char_yrblt,
    NULLIF(ch.char_land_sf, 0.0) AS char_land_sf,
    ch.char_building_pins,
    ch.char_building_pins - ch.char_building_non_units AS char_building_units,
    ch.char_building_non_units,
    ch.bldg_is_mixed_use as char_bldg_is_mixed_use,

    -- Property characteristics from MLS/valuations
    ch.char_building_sf,
    ch.char_unit_sf,
    ch.char_bedrooms,
    ch.char_half_baths,
    ch.char_full_baths,

    -- Land and lot size indicators
    sp.char_land_sf_95_percentile,
    CASE
        WHEN ch.char_land_sf >= sp.char_land_sf_95_percentile THEN true
        ELSE false
    END AS ind_land_gte_95_percentile,

    -- PIN location data for aggregation and spatial joins
    vwlf.census_puma_geoid AS loc_census_puma_geoid,
    vwlf.census_tract_geoid AS loc_census_tract_geoid,
    vwlf.census_data_year AS loc_census_data_year,
    vwlf.census_acs5_puma_geoid AS loc_census_acs5_puma_geoid,
    vwlf.census_acs5_tract_geoid AS loc_census_acs5_tract_geoid,
    vwlf.census_acs5_data_year AS loc_census_acs5_data_year,
    vwlf.tax_municipality_name AS loc_tax_municipality_name,
    vwlf.ward_num AS loc_ward_num,
    vwlf.chicago_community_area_name AS loc_chicago_community_area_name,

    -- Location data used for spatial fixed effects
    vwlf.school_elementary_district_geoid loc_school_elementary_district_geoid,
    vwlf.school_secondary_district_geoid loc_school_secondary_district_geoid,
    vwlf.school_unified_district_geoid AS loc_school_unified_district_geoid,
    vwlf.tax_special_service_area_num AS loc_tax_special_service_area_num,
    vwlf.tax_tif_district_num AS loc_tax_tif_district_num,
    vwlf.misc_subdivision_id AS loc_misc_subdivision_id,

    -- Environmental and access data
    vwlf.env_flood_fema_sfha AS loc_env_flood_fema_sfha,
    vwlf.env_flood_fs_factor AS loc_env_flood_fs_factor,
    vwlf.env_flood_fs_risk_direction AS loc_env_flood_fs_risk_direction,
    vwlf.env_airport_noise_dnl AS loc_env_airport_noise_dnl,
    vwlf.env_ohare_noise_contour_no_buffer_bool AS loc_env_ohare_noise_contour_no_buffer_bool,
    vwlf.access_cmap_walk_nta_score AS loc_access_cmap_walk_nta_score,
    vwlf.access_cmap_walk_total_score AS loc_access_cmap_walk_total_score,

    -- PIN proximity count variables
    vwpf.num_pin_in_half_mile AS prox_num_pin_in_half_mile,
    vwpf.num_bus_stop_in_half_mile AS prox_num_bus_stop_in_half_mile,
    vwpf.num_foreclosure_per_1000_pin_past_5_years AS prox_num_foreclosure_per_1000_pin_past_5_years,
    vwpf.num_school_in_half_mile AS prox_num_school_in_half_mile,
    vwpf.num_school_with_rating_in_half_mile AS prox_num_school_with_rating_in_half_mile,
    vwpf.avg_school_rating_in_half_mile AS prox_avg_school_rating_in_half_mile,

    -- PIN proximity distance variables
    vwpf.nearest_bike_trail_dist_ft AS prox_nearest_bike_trail_dist_ft,
    vwpf.nearest_cemetery_dist_ft AS prox_nearest_cemetery_dist_ft,
    vwpf.nearest_cta_route_dist_ft AS prox_nearest_cta_route_dist_ft,
    vwpf.nearest_cta_stop_dist_ft AS prox_nearest_cta_stop_dist_ft,
    vwpf.nearest_golf_course_dist_ft AS prox_nearest_golf_course_dist_ft,
    vwpf.nearest_hospital_dist_ft AS prox_nearest_hospital_dist_ft,
    vwpf.lake_michigan_dist_ft AS prox_lake_michigan_dist_ft,
    vwpf.nearest_major_road_dist_ft AS prox_nearest_major_road_dist_ft,
    vwpf.nearest_metra_route_dist_ft AS prox_nearest_metra_route_dist_ft,
    vwpf.nearest_metra_stop_dist_ft AS prox_nearest_metra_stop_dist_ft,
    vwpf.nearest_park_dist_ft AS prox_nearest_park_dist_ft,
    vwpf.nearest_railroad_dist_ft AS prox_nearest_railroad_dist_ft,
    vwpf.nearest_water_dist_ft AS prox_nearest_water_dist_ft,

    -- ACS5 census data
    acs5.count_sex_total AS acs5_count_sex_total,
    acs5.percent_age_children AS acs5_percent_age_children,
    acs5.percent_age_senior AS acs5_percent_age_senior,
    acs5.median_age_total AS acs5_median_age_total,
    acs5.percent_mobility_no_move AS acs5_percent_mobility_no_move,
    acs5.percent_mobility_moved_in_county AS acs5_percent_mobility_moved_in_county,
    acs5.percent_mobility_moved_from_other_state AS acs5_percent_mobility_moved_from_other_state,
    acs5.percent_household_family_married AS acs5_percent_household_family_married,
    acs5.percent_household_nonfamily_alone AS acs5_percent_household_nonfamily_alone,
    acs5.percent_education_high_school AS acs5_percent_education_high_school,
    acs5.percent_education_bachelor AS acs5_percent_education_bachelor,
    acs5.percent_education_graduate AS acs5_percent_education_graduate,
    acs5.percent_income_below_poverty_level AS acs5_percent_income_below_poverty_level,
    acs5.median_income_household_past_year AS acs5_median_income_household_past_year,
    acs5.median_income_per_capita_past_year AS acs5_median_income_per_capita_past_year,
    acs5.percent_income_household_received_snap_past_year AS acs5_percent_income_household_received_snap_past_year,
    acs5.percent_employment_unemployed AS acs5_percent_employment_unemployed,
    acs5.median_household_total_occupied_year_built AS acs5_median_household_total_occupied_year_built,
    acs5.median_household_renter_occupied_gross_rent AS acs5_median_household_renter_occupied_gross_rent,
    acs5.median_household_owner_occupied_value AS acs5_median_household_owner_occupied_value,
    acs5.percent_household_owner_occupied AS acs5_percent_household_owner_occupied,
    acs5.percent_household_total_occupied_w_sel_cond AS acs5_percent_household_total_occupied_w_sel_cond,

    -- Institute for Housing Studies data
    housing_index.ihs_avg_year_index AS other_ihs_avg_year_index,
    tbill.tot_tax_amt AS other_tax_bill_amount_total,
    tbill.tax_rate AS other_tax_bill_rate,

    sdre.school_district_avg_rating AS other_school_district_elementary_avg_rating,
    sdrs.school_district_avg_rating AS other_school_district_secondary_avg_rating

FROM uni
LEFT JOIN location.vw_pin10_location_fill vwlf
    ON uni.pin10 = vwlf.pin10
    AND uni.year = vwlf.year
LEFT JOIN proximity.vw_pin10_proximity_fill vwpf
    ON uni.pin10 = vwpf.pin10
    AND uni.year = vwpf.year
LEFT JOIN default.temp_vw_pin_address vwpa
    ON uni.pin = vwpa.pin
    AND uni.year = vwpa.year
LEFT JOIN default.temp_vw_pin_condo_char ch
    ON uni.pin = ch.pin
    AND uni.year = ch.year
LEFT JOIN default.vw_pin_history hist
    ON uni.pin = hist.pin
    AND uni.year = hist.year
LEFT JOIN sqft_percentiles sp
    ON uni.year = sp.year
    AND uni.township_code = sp.township_code
LEFT JOIN acs5
    ON vwlf.census_acs5_tract_geoid = acs5.geoid
    AND vwlf.year = acs5.year
LEFT JOIN housing_index
    ON housing_index.geoid = vwlf.census_puma_geoid
    AND housing_index.year = vwlf.year
LEFT JOIN tax_bill_amount tbill
    ON uni.pin = tbill.pin
    AND uni.year = tbill.year
-- The two following joins need to include year if we get more than
-- one year of school ratings data.
LEFT JOIN (SELECT * FROM school_district_ratings WHERE district_type = 'elementary') sdre
    ON vwlf.school_elementary_district_geoid = sdre.district_geoid
LEFT JOIN (SELECT * FROM school_district_ratings WHERE district_type = 'secondary') sdrs
    ON vwlf.school_secondary_district_geoid = sdrs.district_geoid
