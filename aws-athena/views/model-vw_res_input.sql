/**
View containing cleaned, filled data for residential modeling. Missing data is
filled with the following steps:

1. All historical data is filled FORWARD in time, i.e. data from 2020 fills
   2021 as long as the data isn't something which frequently changes
2. Current data is filled BACKWARD to account for missing historical data.
   Again, this is only for things unlikely to change
3. If any gaps remain, data is filled with the data of the nearest neighboring
   PIN (ONLY for things that don't vary at the property level, such as census
   tract statistics. Property characteristics are NOT filled)

WARNING: This is a very heavy view. Don't use it for anything other than making
extracts for modeling
**/
CREATE OR REPLACE VIEW model.vw_res_input AS
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
),
school_district_ratings AS (
    SELECT
        district_geoid,
        district_type,
        AVG(rating) AS school_district_avg_rating,
        COUNT(*) AS num_schools_in_district,
        year
    FROM other.great_schools_rating
    GROUP BY district_geoid, district_type, year
),
forward_fill AS (
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
        CASE
            WHEN uni.tieback_proration_rate IS NULL THEN 1.0
            ELSE uni.tieback_proration_rate
        END AS meta_tieback_proration_rate,
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
        uni.prop_address_full AS loc_property_address,
        uni.prop_address_city_name AS loc_property_city,
        uni.prop_address_state AS loc_property_state,
        uni.prop_address_zipcode_1 AS loc_property_zip,
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
        CASE
            WHEN uni.cook_municipality_name IS NULL THEN
                LAST_VALUE(uni.cook_municipality_name) IGNORE NULLS
                OVER (PARTITION BY uni.pin ORDER BY uni.year DESC)
            ELSE uni.cook_municipality_name
        END AS loc_cook_municipality_name,
        CASE
            WHEN uni.chicago_ward_num IS NULL THEN
                LAST_VALUE(uni.chicago_ward_num) IGNORE NULLS
                OVER (PARTITION BY uni.pin ORDER BY uni.year DESC)
            ELSE uni.chicago_ward_num
        END AS loc_chicago_ward_num,
        CASE
            WHEN uni.chicago_community_area_name IS NULL THEN
                LAST_VALUE(uni.chicago_community_area_name) IGNORE NULLS
                OVER (PARTITION BY uni.pin ORDER BY uni.year DESC)
            ELSE uni.chicago_community_area_name
        END AS loc_chicago_community_area_name,

        -- Location data used for spatial fixed effects
        CASE
            WHEN uni.school_elementary_district_geoid IS NULL THEN
                LAST_VALUE(uni.school_elementary_district_geoid) IGNORE NULLS
                OVER (PARTITION BY uni.pin ORDER BY uni.year DESC)
            ELSE uni.school_elementary_district_geoid
        END AS loc_school_elementary_district_geoid,
        CASE
            WHEN uni.school_secondary_district_geoid IS NULL THEN
                LAST_VALUE(uni.school_secondary_district_geoid) IGNORE NULLS
                OVER (PARTITION BY uni.pin ORDER BY uni.year DESC)
            ELSE uni.school_secondary_district_geoid
        END AS loc_school_secondary_district_geoid,
        CASE
            WHEN uni.school_unified_district_geoid IS NULL THEN
                LAST_VALUE(uni.school_unified_district_geoid) IGNORE NULLS
                OVER (PARTITION BY uni.pin ORDER BY uni.year DESC)
            ELSE uni.school_unified_district_geoid
        END AS loc_school_unified_district_geoid,
        CASE
            WHEN uni.tax_special_service_area_num IS NULL THEN
                LAST_VALUE(uni.tax_special_service_area_num) IGNORE NULLS
                OVER (PARTITION BY uni.pin ORDER BY uni.year DESC)
            ELSE uni.tax_special_service_area_num
        END AS loc_tax_special_service_area_num,
        CASE
            WHEN uni.tax_tif_district_num IS NULL THEN
                LAST_VALUE(uni.tax_tif_district_num) IGNORE NULLS
                OVER (PARTITION BY uni.pin ORDER BY uni.year DESC)
            ELSE uni.tax_tif_district_num
        END AS loc_tax_tif_district_num,
        CASE
            WHEN uni.misc_subdivision_id IS NULL THEN
                LAST_VALUE(uni.misc_subdivision_id) IGNORE NULLS
                OVER (PARTITION BY uni.pin ORDER BY uni.year DESC)
            ELSE uni.misc_subdivision_id
        END AS loc_misc_subdivision_id,
        CASE
            WHEN uni.misc_unincorporated_area_bool IS NULL THEN
                LAST_VALUE(uni.misc_unincorporated_area_bool) IGNORE NULLS
                OVER (PARTITION BY uni.pin ORDER BY uni.year DESC)
            ELSE uni.misc_unincorporated_area_bool
        END AS loc_misc_unincorporated_area_bool,

        -- Environmental and access data
        CASE
            WHEN uni.env_flood_fema_sfha IS NULL THEN
                LAST_VALUE(uni.env_flood_fema_sfha) IGNORE NULLS
                OVER (PARTITION BY uni.pin ORDER BY uni.year DESC)
            ELSE uni.env_flood_fema_sfha
        END AS loc_env_flood_fema_sfha,
        CASE
            WHEN uni.env_flood_fs_factor IS NULL THEN
                LAST_VALUE(uni.env_flood_fs_factor) IGNORE NULLS
                OVER (PARTITION BY uni.pin ORDER BY uni.year DESC)
            ELSE uni.env_flood_fs_factor
        END AS loc_env_flood_fs_factor,
        CASE
            WHEN uni.env_flood_fs_risk_direction IS NULL THEN
                LAST_VALUE(uni.env_flood_fs_risk_direction) IGNORE NULLS
                OVER (PARTITION BY uni.pin ORDER BY uni.year DESC)
            ELSE uni.env_flood_fs_risk_direction
        END AS loc_env_flood_fs_risk_direction,
        CASE
            WHEN uni.env_ohare_noise_contour_no_buffer_bool IS NULL THEN
                LAST_VALUE(uni.env_ohare_noise_contour_no_buffer_bool) IGNORE NULLS
                OVER (PARTITION BY uni.pin ORDER BY uni.year DESC)
            ELSE uni.env_ohare_noise_contour_no_buffer_bool
        END AS loc_env_ohare_noise_contour_no_buffer_bool,
        CASE
            WHEN uni.access_cmap_walk_nta_score IS NULL THEN
                LAST_VALUE(uni.access_cmap_walk_nta_score) IGNORE NULLS
                OVER (PARTITION BY uni.pin ORDER BY uni.year DESC)
            ELSE uni.access_cmap_walk_nta_score
        END AS loc_access_cmap_walk_nta_score,
        CASE
            WHEN uni.access_cmap_walk_total_score IS NULL THEN
                LAST_VALUE(uni.access_cmap_walk_total_score) IGNORE NULLS
                OVER (PARTITION BY uni.pin ORDER BY uni.year DESC)
            ELSE uni.access_cmap_walk_total_score
        END AS loc_access_cmap_walk_total_score,

        -- PIN proximity count variables
        CASE
            WHEN uni.num_pin_in_half_mile IS NULL THEN
                LAST_VALUE(uni.num_pin_in_half_mile) IGNORE NULLS
                OVER (PARTITION BY uni.pin ORDER BY uni.year DESC)
            ELSE uni.num_pin_in_half_mile
        END AS prox_num_pin_in_half_mile,
        CASE
            WHEN uni.num_bus_stop_in_half_mile IS NULL THEN
                LAST_VALUE(uni.num_bus_stop_in_half_mile) IGNORE NULLS
                OVER (PARTITION BY uni.pin ORDER BY uni.year DESC)
            ELSE uni.num_bus_stop_in_half_mile
        END AS prox_num_bus_stop_in_half_mile,
        CASE
            WHEN uni.num_foreclosure_per_1000_pin_past_5_years IS NULL THEN
                LAST_VALUE(uni.num_foreclosure_per_1000_pin_past_5_years) IGNORE NULLS
                OVER (PARTITION BY uni.pin ORDER BY uni.year DESC)
            ELSE uni.num_foreclosure_per_1000_pin_past_5_years
        END AS prox_num_foreclosure_per_1000_pin_past_5_years,
        CASE
            WHEN uni.num_school_in_half_mile IS NULL THEN
                LAST_VALUE(uni.num_school_in_half_mile) IGNORE NULLS
                OVER (PARTITION BY uni.pin ORDER BY uni.year DESC)
            ELSE uni.num_school_in_half_mile
        END AS prox_num_school_in_half_mile,
        CASE
            WHEN uni.num_school_with_rating_in_half_mile IS NULL THEN
                LAST_VALUE(uni.num_school_with_rating_in_half_mile) IGNORE NULLS
                OVER (PARTITION BY uni.pin ORDER BY uni.year DESC)
            ELSE uni.num_school_with_rating_in_half_mile
        END AS prox_num_school_with_rating_in_half_mile,
        CASE
            WHEN uni.avg_school_rating_in_half_mile IS NULL THEN
                LAST_VALUE(uni.avg_school_rating_in_half_mile) IGNORE NULLS
                OVER (PARTITION BY uni.pin ORDER BY uni.year DESC)
            ELSE uni.avg_school_rating_in_half_mile
        END AS prox_avg_school_rating_in_half_mile,

        -- PIN proximity distance variables
        CASE
            WHEN uni.nearest_bike_trail_dist_ft IS NULL THEN
                LAST_VALUE(uni.nearest_bike_trail_dist_ft) IGNORE NULLS
                OVER (PARTITION BY uni.pin ORDER BY uni.year DESC)
            ELSE uni.nearest_bike_trail_dist_ft
        END AS prox_nearest_bike_trail_dist_ft,
        CASE
            WHEN uni.nearest_cemetery_dist_ft IS NULL THEN
                LAST_VALUE(uni.nearest_cemetery_dist_ft) IGNORE NULLS
                OVER (PARTITION BY uni.pin ORDER BY uni.year DESC)
            ELSE uni.nearest_cemetery_dist_ft
        END AS prox_nearest_cemetery_dist_ft,
        CASE
            WHEN uni.nearest_cta_route_dist_ft IS NULL THEN
                LAST_VALUE(uni.nearest_cta_route_dist_ft) IGNORE NULLS
                OVER (PARTITION BY uni.pin ORDER BY uni.year DESC)
            ELSE uni.nearest_cta_route_dist_ft
        END AS prox_nearest_cta_route_dist_ft,
        CASE
            WHEN uni.nearest_cta_stop_dist_ft IS NULL THEN
                LAST_VALUE(uni.nearest_cta_stop_dist_ft) IGNORE NULLS
                OVER (PARTITION BY uni.pin ORDER BY uni.year DESC)
            ELSE uni.nearest_cta_stop_dist_ft
        END AS prox_nearest_cta_stop_dist_ft,

        CASE
            WHEN uni.nearest_hospital_dist_ft IS NULL THEN
                LAST_VALUE(uni.nearest_hospital_dist_ft) IGNORE NULLS
                OVER (PARTITION BY uni.pin ORDER BY uni.year DESC)
            ELSE uni.nearest_hospital_dist_ft
        END AS prox_nearest_hospital_dist_ft,
        CASE
            WHEN uni.lake_michigan_dist_ft IS NULL THEN
                LAST_VALUE(uni.lake_michigan_dist_ft) IGNORE NULLS
                OVER (PARTITION BY uni.pin ORDER BY uni.year DESC)
            ELSE uni.lake_michigan_dist_ft
        END AS prox_lake_michigan_dist_ft,
        CASE
            WHEN uni.nearest_major_road_dist_ft IS NULL THEN
                LAST_VALUE(uni.nearest_major_road_dist_ft) IGNORE NULLS
                OVER (PARTITION BY uni.pin ORDER BY uni.year DESC)
            ELSE uni.nearest_major_road_dist_ft
        END AS prox_nearest_major_road_dist_ft,
        CASE
            WHEN uni.nearest_metra_route_dist_ft IS NULL THEN
                LAST_VALUE(uni.nearest_metra_route_dist_ft) IGNORE NULLS
                OVER (PARTITION BY uni.pin ORDER BY uni.year DESC)
            ELSE uni.nearest_metra_route_dist_ft
        END AS prox_nearest_metra_route_dist_ft,
        CASE
            WHEN uni.nearest_metra_stop_dist_ft IS NULL THEN
                LAST_VALUE(uni.nearest_metra_stop_dist_ft) IGNORE NULLS
                OVER (PARTITION BY uni.pin ORDER BY uni.year DESC)
            ELSE uni.nearest_metra_stop_dist_ft
        END AS prox_nearest_metra_stop_dist_ft,
        CASE
            WHEN uni.nearest_park_dist_ft IS NULL THEN
                LAST_VALUE(uni.nearest_park_dist_ft) IGNORE NULLS
                OVER (PARTITION BY uni.pin ORDER BY uni.year DESC)
            ELSE uni.nearest_park_dist_ft
        END AS prox_nearest_park_dist_ft,
        CASE
            WHEN uni.nearest_railroad_dist_ft IS NULL THEN
                LAST_VALUE(uni.nearest_railroad_dist_ft) IGNORE NULLS
                OVER (PARTITION BY uni.pin ORDER BY uni.year DESC)
            ELSE uni.nearest_railroad_dist_ft
        END AS prox_nearest_railroad_dist_ft,
        CASE
            WHEN uni.nearest_water_dist_ft IS NULL THEN
                LAST_VALUE(uni.nearest_water_dist_ft) IGNORE NULLS
                OVER (PARTITION BY uni.pin ORDER BY uni.year DESC)
            ELSE uni.nearest_water_dist_ft
        END AS prox_nearest_water_dist_ft,

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
        tbill.tax_rate AS other_tax_bill_rate,
        CASE
            WHEN sdre.school_district_avg_rating IS NULL THEN
                LAST_VALUE(sdre.school_district_avg_rating) IGNORE NULLS
                OVER (PARTITION BY uni.pin ORDER BY uni.year DESC)
            ELSE sdre.school_district_avg_rating
        END AS other_school_district_elementary_avg_rating,
        CASE
            WHEN sdrs.school_district_avg_rating IS NULL THEN
                LAST_VALUE(sdrs.school_district_avg_rating) IGNORE NULLS
                OVER (PARTITION BY uni.pin ORDER BY uni.year DESC)
            ELSE sdrs.school_district_avg_rating
        END AS other_school_district_secondary_avg_rating,

        -- PIN nearest neighbors, used for filling missing data
        uni.nearest_neighbor_1_pin10,
        uni.nearest_neighbor_1_dist_ft,
        uni.nearest_neighbor_2_pin10,
        uni.nearest_neighbor_2_dist_ft,
        uni.nearest_neighbor_3_pin10,
        uni.nearest_neighbor_3_dist_ft

    FROM uni_filtered uni
    LEFT JOIN default.vw_impr_char ch
        ON uni.pin = ch.pin
        AND uni.year = ch.year
    LEFT JOIN default.vw_pin_history hist
        ON uni.pin = hist.pin
        AND uni.year = hist.year
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
    LEFT JOIN (SELECT * FROM school_district_ratings WHERE district_type = 'elementary') sdre
        ON uni.school_elementary_district_geoid = sdre.district_geoid
        AND uni.year = sdre.year
    LEFT JOIN (SELECT * FROM school_district_ratings WHERE district_type = 'secondary') sdrs
        ON uni.school_secondary_district_geoid = sdrs.district_geoid
        AND uni.year = sdrs.year
)
-- Anything with a CASE WHEN here is just borrowing missing values from its nearest
-- spatial neighbor
SELECT
    f1.meta_pin,
    f1.meta_pin10,
    f1.meta_year,
    f1.meta_class,
    f1.meta_modeling_group,
    f1.meta_triad_name,
    f1.meta_triad_code,
    f1.meta_township_name,
    f1.meta_township_code,
    f1.meta_nbhd_code,
    f1.meta_tax_code,
    f1.meta_tieback_key_pin,
    f1.meta_tieback_proration_rate,
    f1.ind_pin_is_prorated,
    f1.meta_card_num,
    f1.ind_pin_is_multicard,
    f1.meta_pin_num_cards,
    f1.ind_pin_is_multiland,
    f1.meta_pin_num_landlines,
    f1.meta_cdu,
    f1.meta_mailed_bldg,
    f1.meta_mailed_land,
    f1.meta_mailed_tot,
    f1.meta_certified_bldg,
    f1.meta_certified_land,
    f1.meta_certified_tot,
    f1.meta_board_bldg,
    f1.meta_board_land,
    f1.meta_board_tot,
    f1.meta_1yr_pri_board_bldg,
    f1.meta_1yr_pri_board_land,
    f1.meta_1yr_pri_board_tot,
    f1.meta_2yr_pri_board_bldg,
    f1.meta_2yr_pri_board_land,
    f1.meta_2yr_pri_board_tot,
    f1.loc_property_address,
    f1.loc_property_city,
    f1.loc_property_state,
    f1.loc_property_zip,
    f1.loc_longitude,
    f1.loc_latitude,
    f1.char_yrblt,
    f1.char_bldg_sf,
    f1.char_land_sf,
    f1.char_beds,
    f1.char_rooms,
    f1.char_fbath,
    f1.char_hbath,
    f1.char_frpl,
    f1.char_type_resd,
    f1.char_cnst_qlty,
    f1.char_apts,
    f1.char_tp_dsgn,
    f1.char_attic_fnsh,
    f1.char_gar1_att,
    f1.char_gar1_area,
    f1.char_gar1_size,
    f1.char_gar1_cnst,
    f1.char_attic_type,
    f1.char_bsmt,
    f1.char_ext_wall,
    f1.char_heat,
    f1.char_bsmt_fin,
    f1.char_roof_cnst,
    f1.char_use,
    f1.char_porch,
    f1.char_air,
    f1.char_tp_plan,
    f1.char_land_sf_95_percentile,
    f1.ind_land_gte_95_percentile,
    f1.char_bldg_sf_95_percentile,
    f1.ind_bldg_gte_95_percentile,
    f1.ind_land_bldg_ratio_gt_4,
    f1.loc_census_puma_geoid,
    f1.loc_census_tract_geoid,
    f1.loc_census_data_year,
    f1.loc_census_acs5_puma_geoid,
    f1.loc_census_acs5_tract_geoid,
    f1.loc_census_acs5_data_year,
    CASE
        WHEN f1.loc_cook_municipality_name IS NOT NULL THEN f1.loc_cook_municipality_name
        WHEN f1.loc_cook_municipality_name IS NULL THEN nn1.loc_cook_municipality_name
        WHEN nn1.loc_cook_municipality_name IS NULL THEN nn2.loc_cook_municipality_name
        ELSE NULL
    END AS loc_cook_municipality_name,
    CASE
        WHEN f1.loc_chicago_ward_num IS NOT NULL THEN f1.loc_chicago_ward_num
        WHEN f1.loc_chicago_ward_num IS NULL THEN nn1.loc_chicago_ward_num
        WHEN nn1.loc_chicago_ward_num IS NULL THEN nn2.loc_chicago_ward_num
        ELSE NULL
    END AS loc_chicago_ward_num,
    CASE
        WHEN f1.loc_chicago_community_area_name IS NOT NULL THEN f1.loc_chicago_community_area_name
        WHEN f1.loc_chicago_community_area_name IS NULL THEN nn1.loc_chicago_community_area_name
        WHEN nn1.loc_chicago_community_area_name IS NULL THEN nn2.loc_chicago_community_area_name
        ELSE NULL
    END AS loc_chicago_community_area_name,
    CASE
        WHEN f1.loc_school_elementary_district_geoid IS NOT NULL THEN f1.loc_school_elementary_district_geoid
        WHEN f1.loc_school_elementary_district_geoid IS NULL THEN nn1.loc_school_elementary_district_geoid
        WHEN nn1.loc_school_elementary_district_geoid IS NULL THEN nn2.loc_school_elementary_district_geoid
        ELSE NULL
    END AS loc_school_elementary_district_geoid,
    CASE
        WHEN f1.loc_school_secondary_district_geoid IS NOT NULL THEN f1.loc_school_secondary_district_geoid
        WHEN f1.loc_school_secondary_district_geoid IS NULL THEN nn1.loc_school_secondary_district_geoid
        WHEN nn1.loc_school_secondary_district_geoid IS NULL THEN nn2.loc_school_secondary_district_geoid
        ELSE NULL
    END AS loc_school_secondary_district_geoid,
    CASE
        WHEN f1.loc_school_unified_district_geoid IS NOT NULL THEN f1.loc_school_unified_district_geoid
        WHEN f1.loc_school_unified_district_geoid IS NULL THEN nn1.loc_school_unified_district_geoid
        WHEN nn1.loc_school_unified_district_geoid IS NULL THEN nn2.loc_school_unified_district_geoid
        ELSE NULL
    END AS loc_school_unified_district_geoid,
    CASE
        WHEN f1.loc_tax_special_service_area_num IS NOT NULL THEN f1.loc_tax_special_service_area_num
        WHEN f1.loc_tax_special_service_area_num IS NULL THEN nn1.loc_tax_special_service_area_num
        WHEN nn1.loc_tax_special_service_area_num IS NULL THEN nn2.loc_tax_special_service_area_num
        ELSE NULL
    END AS loc_tax_special_service_area_num,
    CASE
        WHEN f1.loc_tax_tif_district_num IS NOT NULL THEN f1.loc_tax_tif_district_num
        WHEN f1.loc_tax_tif_district_num IS NULL THEN nn1.loc_tax_tif_district_num
        WHEN nn1.loc_tax_tif_district_num IS NULL THEN nn2.loc_tax_tif_district_num
        ELSE NULL
    END AS loc_tax_tif_district_num,
    CASE
        WHEN f1.loc_misc_subdivision_id IS NOT NULL THEN f1.loc_misc_subdivision_id
        WHEN f1.loc_misc_subdivision_id IS NULL THEN nn1.loc_misc_subdivision_id
        WHEN nn1.loc_misc_subdivision_id IS NULL THEN nn2.loc_misc_subdivision_id
        ELSE NULL
    END AS loc_misc_subdivision_id,
    CASE
        WHEN f1.loc_misc_unincorporated_area_bool IS NOT NULL THEN f1.loc_misc_unincorporated_area_bool
        WHEN f1.loc_misc_unincorporated_area_bool IS NULL THEN nn1.loc_misc_unincorporated_area_bool
        WHEN nn1.loc_misc_unincorporated_area_bool IS NULL THEN nn2.loc_misc_unincorporated_area_bool
        ELSE NULL
    END AS loc_misc_unincorporated_area_bool,
    CASE
        WHEN f1.loc_env_flood_fema_sfha IS NOT NULL THEN f1.loc_env_flood_fema_sfha
        WHEN f1.loc_env_flood_fema_sfha IS NULL THEN nn1.loc_env_flood_fema_sfha
        WHEN nn1.loc_env_flood_fema_sfha IS NULL THEN nn2.loc_env_flood_fema_sfha
        ELSE NULL
    END AS loc_env_flood_fema_sfha,
    CASE
        WHEN f1.loc_env_flood_fs_factor IS NOT NULL THEN f1.loc_env_flood_fs_factor
        WHEN f1.loc_env_flood_fs_factor IS NULL THEN nn1.loc_env_flood_fs_factor
        WHEN nn1.loc_env_flood_fs_factor IS NULL THEN nn2.loc_env_flood_fs_factor
        ELSE NULL
    END AS loc_env_flood_fs_factor,
    CASE
        WHEN f1.loc_env_flood_fs_risk_direction IS NOT NULL THEN f1.loc_env_flood_fs_risk_direction
        WHEN f1.loc_env_flood_fs_risk_direction IS NULL THEN nn1.loc_env_flood_fs_risk_direction
        WHEN nn1.loc_env_flood_fs_risk_direction IS NULL THEN nn2.loc_env_flood_fs_risk_direction
        ELSE NULL
    END AS loc_env_flood_fs_risk_direction,
    CASE
        WHEN f1.loc_env_ohare_noise_contour_no_buffer_bool IS NOT NULL THEN f1.loc_env_ohare_noise_contour_no_buffer_bool
        WHEN f1.loc_env_ohare_noise_contour_no_buffer_bool IS NULL THEN nn1.loc_env_ohare_noise_contour_no_buffer_bool
        WHEN nn1.loc_env_ohare_noise_contour_no_buffer_bool IS NULL THEN nn2.loc_env_ohare_noise_contour_no_buffer_bool
        ELSE NULL
    END AS loc_env_ohare_noise_contour_no_buffer_bool,
    CASE
        WHEN f1.loc_access_cmap_walk_nta_score IS NOT NULL THEN f1.loc_access_cmap_walk_nta_score
        WHEN f1.loc_access_cmap_walk_nta_score IS NULL THEN nn1.loc_access_cmap_walk_nta_score
        WHEN nn1.loc_access_cmap_walk_nta_score IS NULL THEN nn2.loc_access_cmap_walk_nta_score
        ELSE NULL
    END AS loc_access_cmap_walk_nta_score,
    CASE
        WHEN f1.loc_access_cmap_walk_total_score IS NOT NULL THEN f1.loc_access_cmap_walk_total_score
        WHEN f1.loc_access_cmap_walk_total_score IS NULL THEN nn1.loc_access_cmap_walk_total_score
        WHEN nn1.loc_access_cmap_walk_total_score IS NULL THEN nn2.loc_access_cmap_walk_total_score
        ELSE NULL
    END AS loc_access_cmap_walk_total_score,
    f1.prox_num_pin_in_half_mile,
    f1.prox_num_bus_stop_in_half_mile,
    f1.prox_num_foreclosure_per_1000_pin_past_5_years,
    CASE
        WHEN f1.prox_num_school_in_half_mile IS NOT NULL THEN f1.prox_num_school_in_half_mile
        WHEN f1.prox_num_school_in_half_mile IS NULL THEN nn1.prox_num_school_in_half_mile
        WHEN nn1.prox_num_school_in_half_mile IS NULL THEN nn2.prox_num_school_in_half_mile
        ELSE NULL
    END AS prox_num_school_in_half_mile,
    CASE
        WHEN f1.prox_num_school_with_rating_in_half_mile IS NOT NULL THEN f1.prox_num_school_with_rating_in_half_mile
        WHEN f1.prox_num_school_with_rating_in_half_mile IS NULL THEN nn1.prox_num_school_with_rating_in_half_mile
        WHEN nn1.prox_num_school_with_rating_in_half_mile IS NULL THEN nn2.prox_num_school_with_rating_in_half_mile
        ELSE NULL
    END AS prox_num_school_with_rating_in_half_mile,
    CASE
        WHEN f1.prox_avg_school_rating_in_half_mile IS NOT NULL THEN f1.prox_avg_school_rating_in_half_mile
        WHEN f1.prox_avg_school_rating_in_half_mile IS NULL THEN nn1.prox_avg_school_rating_in_half_mile
        WHEN nn1.prox_avg_school_rating_in_half_mile IS NULL THEN nn2.prox_avg_school_rating_in_half_mile
        ELSE NULL
    END AS prox_avg_school_rating_in_half_mile,
    CASE
        WHEN f1.prox_nearest_bike_trail_dist_ft IS NOT NULL THEN f1.prox_nearest_bike_trail_dist_ft
        WHEN f1.prox_nearest_bike_trail_dist_ft IS NULL THEN nn1.prox_nearest_bike_trail_dist_ft
        WHEN nn1.prox_nearest_bike_trail_dist_ft IS NULL THEN nn2.prox_nearest_bike_trail_dist_ft
        ELSE NULL
    END AS prox_nearest_bike_trail_dist_ft,
    CASE
        WHEN f1.prox_nearest_cemetery_dist_ft IS NOT NULL THEN f1.prox_nearest_cemetery_dist_ft
        WHEN f1.prox_nearest_cemetery_dist_ft IS NULL THEN nn1.prox_nearest_cemetery_dist_ft
        WHEN nn1.prox_nearest_cemetery_dist_ft IS NULL THEN nn2.prox_nearest_cemetery_dist_ft
        ELSE NULL
    END AS prox_nearest_cemetery_dist_ft,
    f1.prox_nearest_cta_route_dist_ft,
    f1.prox_nearest_cta_stop_dist_ft,
    CASE
        WHEN f1.prox_nearest_hospital_dist_ft IS NOT NULL THEN f1.prox_nearest_hospital_dist_ft
        WHEN f1.prox_nearest_hospital_dist_ft IS NULL THEN nn1.prox_nearest_hospital_dist_ft
        WHEN nn1.prox_nearest_hospital_dist_ft IS NULL THEN nn2.prox_nearest_hospital_dist_ft
        ELSE NULL
    END AS prox_nearest_hospital_dist_ft,
    CASE
        WHEN f1.prox_lake_michigan_dist_ft IS NOT NULL THEN f1.prox_lake_michigan_dist_ft
        WHEN f1.prox_lake_michigan_dist_ft IS NULL THEN nn1.prox_lake_michigan_dist_ft
        WHEN nn1.prox_lake_michigan_dist_ft IS NULL THEN nn2.prox_lake_michigan_dist_ft
        ELSE NULL
    END AS prox_lake_michigan_dist_ft,
    CASE
        WHEN f1.prox_nearest_major_road_dist_ft IS NOT NULL THEN f1.prox_nearest_major_road_dist_ft
        WHEN f1.prox_nearest_major_road_dist_ft IS NULL THEN nn1.prox_nearest_major_road_dist_ft
        WHEN nn1.prox_nearest_major_road_dist_ft IS NULL THEN nn2.prox_nearest_major_road_dist_ft
        ELSE NULL
    END AS prox_nearest_major_road_dist_ft,
    f1.prox_nearest_metra_route_dist_ft,
    f1.prox_nearest_metra_stop_dist_ft,
    CASE
        WHEN f1.prox_nearest_park_dist_ft IS NOT NULL THEN f1.prox_nearest_park_dist_ft
        WHEN f1.prox_nearest_park_dist_ft IS NULL THEN nn1.prox_nearest_park_dist_ft
        WHEN nn1.prox_nearest_park_dist_ft IS NULL THEN nn2.prox_nearest_park_dist_ft
        ELSE NULL
    END AS prox_nearest_park_dist_ft,
    CASE
        WHEN f1.prox_nearest_railroad_dist_ft IS NOT NULL THEN f1.prox_nearest_railroad_dist_ft
        WHEN f1.prox_nearest_railroad_dist_ft IS NULL THEN nn1.prox_nearest_railroad_dist_ft
        WHEN nn1.prox_nearest_railroad_dist_ft IS NULL THEN nn2.prox_nearest_railroad_dist_ft
        ELSE NULL
    END AS prox_nearest_railroad_dist_ft,
    CASE
        WHEN f1.prox_nearest_water_dist_ft IS NOT NULL THEN f1.prox_nearest_water_dist_ft
        WHEN f1.prox_nearest_water_dist_ft IS NULL THEN nn1.prox_nearest_water_dist_ft
        WHEN nn1.prox_nearest_water_dist_ft IS NULL THEN nn2.prox_nearest_water_dist_ft
        ELSE NULL
    END AS prox_nearest_water_dist_ft,
    f1.acs5_count_sex_total,
    f1.acs5_percent_age_children,
    f1.acs5_percent_age_senior,
    f1.acs5_median_age_total,
    f1.acs5_percent_race_white,
    f1.acs5_percent_race_black,
    f1.acs5_percent_race_hisp,
    f1.acs5_percent_race_aian,
    f1.acs5_percent_race_asian,
    f1.acs5_percent_race_nhpi,
    f1.acs5_percent_race_other,
    f1.acs5_percent_mobility_no_move,
    f1.acs5_percent_mobility_moved_from_other_state,
    f1.acs5_percent_household_family_married,
    f1.acs5_percent_household_nonfamily_alone,
    f1.acs5_percent_education_high_school,
    f1.acs5_percent_education_bachelor,
    f1.acs5_percent_education_graduate,
    f1.acs5_percent_income_below_poverty_level,
    f1.acs5_median_income_household_past_year,
    f1.acs5_median_income_per_capita_past_year,
    f1.acs5_percent_income_household_received_snap_past_year,
    f1.acs5_percent_employment_unemployed,
    f1.acs5_median_household_total_occupied_year_built,
    f1.acs5_median_household_renter_occupied_gross_rent,
    f1.acs5_median_household_owner_occupied_value,
    f1.acs5_percent_household_owner_occupied,
    f1.acs5_percent_household_total_occupied_w_sel_cond,
    f1.other_ihs_avg_year_index,
    f1.other_tax_bill_amount_total,
    f1.other_tax_bill_rate,
    CASE
        WHEN f1.other_school_district_elementary_avg_rating IS NOT NULL THEN f1.other_school_district_elementary_avg_rating
        WHEN f1.other_school_district_elementary_avg_rating IS NULL THEN nn1.other_school_district_elementary_avg_rating
        WHEN nn1.other_school_district_elementary_avg_rating IS NULL THEN nn2.other_school_district_elementary_avg_rating
        ELSE NULL
    END AS other_school_district_elementary_avg_rating,
    CASE
        WHEN f1.other_school_district_secondary_avg_rating IS NOT NULL THEN f1.other_school_district_secondary_avg_rating
        WHEN f1.other_school_district_secondary_avg_rating IS NULL THEN nn1.other_school_district_secondary_avg_rating
        WHEN nn1.other_school_district_secondary_avg_rating IS NULL THEN nn2.other_school_district_secondary_avg_rating
        ELSE NULL
    END AS other_school_district_secondary_avg_rating
FROM forward_fill f1
LEFT JOIN (
    SELECT *
    FROM forward_fill
    WHERE NOT ind_pin_is_multicard
    AND nearest_neighbor_1_dist_ft <= 500
) nn1
    ON f1.nearest_neighbor_1_pin10 = nn1.meta_pin10
    AND f1.meta_year = nn1.meta_year
LEFT JOIN (
    SELECT *
    FROM forward_fill
    WHERE NOT ind_pin_is_multicard
    AND nearest_neighbor_2_dist_ft <= 500
) nn2
    ON f1.nearest_neighbor_1_pin10 = nn2.meta_pin10
    AND f1.meta_year = nn2.meta_year