CREATE OR REPLACE VIEW model.vw_res_input_raw AS
WITH uni_filtered AS (
    SELECT *
    FROM default.vw_pin_universe
    WHERE class IN (
        '202', '203', '204', '205', '206', '207', '208', '209',
        '210', '211', '212', '218', '219', '234', '278', '295'
    )
),
distinct_years AS (
    SELECT DISTINCT year
    FROM uni_filtered
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
        uni.year, uni.township_code,
        CAST(approx_percentile(ch.char_bldg_sf, 0.95) AS int) AS char_bldg_sf_95_percentile,
        CAST(approx_percentile(ch.char_land_sf, 0.95) AS int) AS char_land_sf_95_percentile
    FROM uni_filtered uni
    LEFT JOIN default.vw_impr_char ch
        ON uni.pin = ch.pin
        AND uni.year = ch.year
    GROUP BY uni.year, uni.township_code
),
tax_bill_amount_fill AS (
    SELECT
        fill_years.pin_year,
        fill_data.year,
        fill_data.pin,
        fill_data.tot_tax_amt,
        fill_data.amt_tax_paid,
        fill_data.tax_rate
    FROM (
        SELECT
        dy.year AS pin_year, MAX(df.year) AS fill_year
        FROM tax.bill_amount df
        CROSS JOIN distinct_years dy
        WHERE dy.year >= df.year
        GROUP BY dy.year
    ) fill_years
    LEFT JOIN tax.bill_amount fill_data
        ON fill_years.fill_year = fill_data.year
)
SELECT
    uni.pin AS meta_pin,
    uni.pin10 AS meta_pin10,
    uni.year AS meta_year,
    uni.class AS meta_class,
    CASE
        WHEN uni.class IN ('211', '212') THEN 'MF'
        WHEN uni.class IN ('218', '219') THEN 'BB'
        ELSE 'SF'
    END AS meta_modeling_group,
    uni.triad_name AS meta_triad_name,
    uni.triad_code AS meta_triad_code,
    uni.township_name AS meta_township_name,
    uni.township_code AS meta_township_code,
    uni.nbhd_code AS meta_nbhd_code,
    uni.tax_code AS meta_tax_code,

    -- Proration fields. Buildings can be split over multiple PINs, with each
    -- PIN owning a percentage of a building
    uni.tieback_key_pin AS meta_tieback_key_pin,
    uni.tieback_proration_rate AS meta_tieback_proration_rate,
    CASE
        WHEN uni.tieback_proration_rate < 1.0 THEN true
        ELSE false
    END AS ind_pin_is_prorated,

    -- Multicode / multi-landline related fields. Each PIN can have more than
    -- one improvement AND/OR more than one attached landline
    ch.card AS meta_card_num,
    ch.pin_is_multicard AS ind_pin_is_multicard,
    ch.pin_num_cards AS meta_pin_num_cards,
    ch.pin_is_multiland AS ind_pin_is_multiland,
    ch.pin_num_landlines AS meta_pin_num_landlines,
    ch.cdu AS meta_cdu,

    -- Individual PIN-level address/location
    NULLIF(CONCAT_WS(
        ' ',
        uni.address_prefix, CAST(uni.address_street_number AS varchar),
        uni.address_street_dir, uni.address_street_name,
        uni.address_suffix_1
    ), '') AS loc_property_address,
    NULLIF(CONCAT_WS(
        ' ',
        uni.address_unit_prefix, uni.address_unit_number
    ), '') AS loc_property_apt_no,
    uni.address_city_name AS loc_property_city,
    uni.address_state AS loc_property_state,
    uni.address_zipcode_1 AS loc_property_zip,
    uni.lon AS loc_longitude,
    uni.lat AS loc_latitude,

    -- Property characteristics from iasWorld
    ch.char_yrblt,
    ch.char_bldg_sf,
    ch.char_land_sf,
    ch.char_beds,
    ch.char_rooms,
    ch.char_fbath,
    ch.char_hbath,
    ch.char_frpl,
    ch.char_type_resd,
    ch.char_cnst_qlty,
    ch.char_apts,
    ch.char_tp_dsgn,
    ch.char_attic_fnsh,
    ch.char_gar1_att,
    ch.char_gar1_area,
    ch.char_gar1_size,
    ch.char_gar1_cnst,
    ch.char_attic_type,
    ch.char_bsmt,
    ch.char_ext_wall,
    ch.char_heat,
    ch.char_bsmt_fin,
    ch.char_roof_cnst,
    ch.char_use,
    ch.char_porch,
    ch.char_air,
    ch.char_tp_plan,

    -- Land and lot size indicators
    sp.char_land_sf_95_percentile,
    CASE
        WHEN ch.char_land_sf >= sp.char_land_sf_95_percentile THEN true
        ELSE false
    END AS ind_land_gte_95_percentile,
    sp.char_bldg_sf_95_percentile,
    CASE
        WHEN ch.char_bldg_sf >= sp.char_bldg_sf_95_percentile THEN true
        ELSE false
    END AS ind_bldg_gte_95_percentile,
    CASE
        WHEN (ch.char_land_sf / (ch.char_bldg_sf + 1)) > 4.0 THEN true
        ELSE false
    END AS ind_land_bldg_ratio_gt_4,

    -- PIN location data for aggregation and spatial joins
    uni.census_puma_geoid AS loc_census_puma_geoid,
    uni.census_tract_geoid AS loc_census_tract_geoid,
    uni.census_data_year AS loc_census_data_year,
    uni.census_acs5_puma_geoid AS loc_census_acs5_puma_geoid,
    uni.census_acs5_tract_geoid AS loc_census_acs5_tract_geoid,
    uni.census_acs5_data_year AS loc_census_acs5_data_year,
    uni.cook_municipality_name AS loc_cook_municipality_name,
    uni.chicago_ward_num AS loc_chicago_ward_num,
    uni.chicago_community_area_name AS loc_chicago_community_area_name,

    -- Location data used for spatial fixed effects
    uni.school_elementary_district_geoid AS loc_school_elementary_district_geoid,
    uni.school_secondary_district_geoid AS loc_school_secondary_district_geoid,
    uni.school_unified_district_geoid AS loc_school_unified_district_geoid,
    uni.tax_special_service_area_num AS loc_tax_special_service_area_num,
    uni.tax_tif_district_num AS loc_tax_tif_district_num,
    uni.misc_subdivision_id AS loc_misc_subdivision_id,
    uni.misc_unincorporated_area_bool AS loc_misc_unincorporated_area_bool,

    -- Environmental and access data
    uni.env_flood_fema_sfha AS loc_env_flood_fema_sfha,
    uni.env_flood_fs_factor AS loc_env_flood_fs_factor,
    uni.env_flood_fs_risk_direction AS loc_env_flood_fs_risk_direction,
    uni.env_ohare_noise_contour_no_buffer_bool AS loc_env_ohare_noise_contour_no_buffer_bool,
    uni.access_cmap_walk_nta_score AS loc_access_cmap_walk_nta_score,
    uni.access_cmap_walk_total_score AS loc_access_cmap_walk_total_score,

    -- PIN proximity count variables
    uni.num_pin_in_half_mile AS prox_num_pin_in_half_mile,
    uni.num_bus_stop_in_half_mile AS prox_num_bus_stop_in_half_mile,
    uni.num_foreclosure_per_1000_pin_past_5_years AS prox_num_foreclosure_per_1000_pin_past_5_years,
    uni.num_school_in_half_mile AS prox_num_school_in_half_mile,
    uni.avg_school_rating_in_half_mile AS prox_avg_school_rating_in_half_mile,

    -- PIN proximity distance variables
    uni.nearest_bike_trail_dist_ft AS prox_nearest_bike_trail_dist_ft,
    uni.nearest_cemetery_dist_ft AS prox_nearest_cemetery_dist_ft,
    uni.nearest_cta_route_dist_ft AS prox_nearest_cta_route_dist_ft,
    uni.nearest_cta_stop_dist_ft AS prox_nearest_cta_stop_dist_ft,
    uni.nearest_hospital_dist_ft AS prox_nearest_hospital_dist_ft,
    uni.lake_michigan_dist_ft AS prox_lake_michigan_dist_ft,
    uni.nearest_major_road_dist_ft AS prox_nearest_major_road_dist_ft,
    uni.nearest_metra_route_dist_ft AS prox_nearest_metra_route_dist_ft,
    uni.nearest_metra_stop_dist_ft AS prox_nearest_metra_stop_dist_ft,
    uni.nearest_park_dist_ft AS prox_nearest_park_dist_ft,
    uni.nearest_railroad_dist_ft AS prox_nearest_railroad_dist_ft,
    uni.nearest_water_dist_ft AS prox_nearest_water_dist_ft,

    -- ACS5 census data
    acs5.count_sex_total AS acs5_count_sex_total,
    acs5.percent_age_children AS acs5_percent_age_children,
    acs5.percent_age_senior AS acs5_percent_age_senior,
    acs5.median_age_total AS acs5_median_age_total,
    acs5.percent_race_white AS acs5_percent_race_white,
    acs5.percent_race_black AS acs5_percent_race_black,
    acs5.percent_race_hisp AS acs5_percent_race_hisp,
    acs5.percent_race_aian AS acs5_percent_race_aian,
    acs5.percent_race_asian AS acs5_percent_race_asian,
    acs5.percent_race_nhpi AS acs5_percent_race_nhpi,
    acs5.percent_race_other AS acs5_percent_race_other,
    acs5.percent_mobility_no_move AS acs5_percent_mobility_no_move,
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
    tbill.amt_tax_paid AS other_tax_bill_amount_paid,
    tbill.tax_rate AS other_tax_bill_rate

FROM uni_filtered uni
LEFT JOIN default.vw_impr_char ch
    ON uni.pin = ch.pin
    AND uni.year = ch.year
LEFT JOIN sqft_percentiles sp
    ON uni.year = sp.year
    AND uni.township_code = sp.township_code
LEFT JOIN acs5
    ON uni.census_acs5_tract_geoid = acs5.geoid
    AND uni.census_acs5_data_year = acs5.acs5_data_year
    AND uni.year = acs5.year
LEFT JOIN housing_index
    ON housing_index.geoid = uni.census_puma_geoid
    AND housing_index.year = uni.year
LEFT JOIN tax_bill_amount_fill tbill
    ON uni.pin = tbill.pin
    AND uni.year = tbill.pin_year