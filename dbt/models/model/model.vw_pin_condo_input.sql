/*
View containing cleaned, filled data for residential condo modeling. Missing
data is filled with the following steps:

1. All historical data is filled FORWARD in time, i.e. data from 2020 fills
   2021 as long as the data isn't something which frequently changes
2. Current data is filled BACKWARD to account for missing historical data.
   Again, this is only for things unlikely to change

This view is "materialized" (made into a table) daily in order to improve
query performance and reduce data queried by Athena. The materialization
is triggered by sqoop-bot (runs after Sqoop grabs iasWorld data)
*/
{{
    config(
        materialized='table',
        partitioned_by=['year'],
        bucketed_by=['meta_pin'],
        bucket_count=1
    )
}}

WITH uni AS (
    SELECT
        vpsi.meta_pin,
        vpsi.meta_pin10,
        vpsi.meta_year,
        vpsi.meta_class,
        vpsi.meta_triad_name,
        vpsi.meta_triad_code,
        vpsi.meta_township_name,
        vpsi.meta_township_code,
        vpsi.meta_nbhd_code,
        vpsi.meta_tax_code,
        vpsi.meta_mailed_bldg,
        vpsi.meta_mailed_land,
        vpsi.meta_mailed_tot,
        vpsi.meta_certified_bldg,
        vpsi.meta_certified_land,
        vpsi.meta_certified_tot,
        vpsi.meta_board_bldg,
        vpsi.meta_board_land,
        vpsi.meta_board_tot,
        vpsi.meta_1yr_pri_board_bldg,
        vpsi.meta_1yr_pri_board_land,
        vpsi.meta_1yr_pri_board_tot,
        vpsi.meta_2yr_pri_board_bldg,
        vpsi.meta_2yr_pri_board_land,
        vpsi.meta_2yr_pri_board_tot,
        vpsi.loc_property_address,
        vpsi.loc_property_city,
        vpsi.loc_property_state,
        vpsi.loc_property_zip,
        vpsi.loc_longitude,
        vpsi.loc_latitude,
        vpsi.loc_x_3435,
        vpsi.loc_y_3435,
        vpsi.loc_census_puma_geoid,
        vpsi.loc_census_tract_geoid,
        vpsi.loc_census_data_year,
        vpsi.loc_census_acs5_puma_geoid,
        vpsi.loc_census_acs5_tract_geoid,
        vpsi.loc_census_acs5_data_year,
        vpsi.loc_tax_municipality_name,
        vpsi.loc_ward_num,
        vpsi.loc_chicago_community_area_name,
        vpsi.loc_school_elementary_district_geoid,
        vpsi.loc_school_secondary_district_geoid,
        vpsi.loc_school_unified_district_geoid,
        vpsi.loc_tax_special_service_area_num,
        vpsi.loc_tax_tif_district_num,
        vpsi.loc_misc_subdivision_id,
        vpsi.loc_env_flood_fema_sfha,
        vpsi.loc_env_flood_fs_factor,
        vpsi.loc_env_flood_fs_risk_direction,
        vpsi.loc_env_ohare_noise_contour_no_buffer_bool,
        vpsi.loc_env_airport_noise_dnl,
        vpsi.loc_access_cmap_walk_nta_score,
        vpsi.loc_access_cmap_walk_total_score,
        vpsi.prox_num_pin_in_half_mile,
        vpsi.prox_num_bus_stop_in_half_mile,
        vpsi.prox_num_foreclosure_per_1000_pin_past_5_years,
        vpsi.prox_num_school_in_half_mile,
        vpsi.prox_num_school_with_rating_in_half_mile,
        vpsi.prox_avg_school_rating_in_half_mile,
        vpsi.prox_airport_dnl_total,
        vpsi.prox_nearest_bike_trail_dist_ft,
        vpsi.prox_nearest_cemetery_dist_ft,
        vpsi.prox_nearest_cta_route_dist_ft,
        vpsi.prox_nearest_cta_stop_dist_ft,
        vpsi.prox_nearest_golf_course_dist_ft,
        vpsi.prox_nearest_grocery_store_dist_ft,
        vpsi.prox_nearest_hospital_dist_ft,
        vpsi.prox_lake_michigan_dist_ft,
        vpsi.prox_nearest_major_road_dist_ft,
        vpsi.prox_nearest_metra_route_dist_ft,
        vpsi.prox_nearest_metra_stop_dist_ft,
        vpsi.prox_nearest_new_construction_dist_ft,
        vpsi.prox_nearest_park_dist_ft,
        vpsi.prox_nearest_railroad_dist_ft,
        vpsi.prox_nearest_road_arterial_daily_traffic,
        vpsi.prox_nearest_road_arterial_dist_ft,
        vpsi.prox_nearest_road_arterial_lanes,
        vpsi.prox_nearest_road_arterial_speed_limit,
        vpsi.prox_nearest_road_arterial_surface_type,
        vpsi.prox_nearest_road_collector_daily_traffic,
        vpsi.prox_nearest_road_collector_dist_ft,
        vpsi.prox_nearest_road_collector_lanes,
        vpsi.prox_nearest_road_collector_speed_limit,
        vpsi.prox_nearest_road_collector_surface_type,
        vpsi.prox_nearest_road_highway_daily_traffic,
        vpsi.prox_nearest_road_highway_dist_ft,
        vpsi.prox_nearest_road_highway_lanes,
        vpsi.prox_nearest_road_highway_speed_limit,
        vpsi.prox_nearest_road_highway_surface_type,
        vpsi.prox_nearest_secondary_road_dist_ft,
        vpsi.prox_nearest_stadium_dist_ft,
        vpsi.prox_nearest_university_dist_ft,
        vpsi.prox_nearest_vacant_land_dist_ft,
        vpsi.prox_nearest_water_dist_ft,
        vpsi.shp_parcel_centroid_dist_ft_sd,
        vpsi.shp_parcel_edge_len_ft_sd,
        vpsi.shp_parcel_interior_angle_sd,
        vpsi.shp_parcel_mrr_area_ratio,
        vpsi.shp_parcel_mrr_side_ratio,
        vpsi.shp_parcel_num_vertices,
        vpsi.acs5_count_sex_total,
        vpsi.acs5_percent_age_children,
        vpsi.acs5_percent_age_senior,
        vpsi.acs5_median_age_total,
        vpsi.acs5_percent_mobility_no_move,
        vpsi.acs5_percent_mobility_moved_in_county,
        vpsi.acs5_percent_mobility_moved_from_other_state,
        vpsi.acs5_percent_household_family_married,
        vpsi.acs5_percent_household_nonfamily_alone,
        vpsi.acs5_percent_education_high_school,
        vpsi.acs5_percent_education_bachelor,
        vpsi.acs5_percent_education_graduate,
        vpsi.acs5_percent_income_below_poverty_level,
        vpsi.acs5_median_income_household_past_year,
        vpsi.acs5_median_income_per_capita_past_year,
        vpsi.acs5_percent_income_household_received_snap_past_year,
        vpsi.acs5_percent_employment_unemployed,
        vpsi.acs5_median_household_total_occupied_year_built,
        vpsi.acs5_median_household_renter_occupied_gross_rent,
        vpsi.acs5_median_household_owner_occupied_value,
        vpsi.acs5_percent_household_owner_occupied,
        vpsi.acs5_percent_household_total_occupied_w_sel_cond,
        vpsi.other_ihs_avg_year_index,
        vpsi.other_distressed_community_index,
        vpsi.other_affordability_risk_index,
        vpsi.other_tax_bill_amount_total,
        vpsi.other_tax_bill_rate,
        vpsi.other_school_district_elementary_avg_rating,
        vpsi.other_school_district_secondary_avg_rating,
        -- Exemptions data is usually missing for the 1 or 2 years prior
        -- to the lien date, so we need to fill missing values w/ down up fill
        -- This assumes that people currently receiving exemptions keep them
        COALESCE(
            vpsi.ccao_is_active_exe_homeowner,
            LAG(vpsi.ccao_is_active_exe_homeowner)
                IGNORE NULLS
                OVER (PARTITION BY vpsi.meta_pin ORDER BY vpsi.meta_year)
        ) AS ccao_is_active_exe_homeowner,
        COALESCE(
            vpsi.ccao_n_years_exe_homeowner,
            LAG(vpsi.ccao_n_years_exe_homeowner)
                IGNORE NULLS
                OVER (PARTITION BY vpsi.meta_pin ORDER BY vpsi.meta_year)
        ) AS ccao_n_years_exe_homeowner,
        vpsi.ccao_is_corner_lot,
        vpsi.nearest_neighbor_1_pin10,
        vpsi.nearest_neighbor_1_dist_ft,
        vpsi.nearest_neighbor_2_pin10,
        vpsi.nearest_neighbor_2_dist_ft,
        vpsi.nearest_neighbor_3_pin10,
        vpsi.nearest_neighbor_3_dist_ft
    FROM {{ ref('model.vw_pin_shared_input') }} AS vpsi
    WHERE vpsi.meta_class IN ('299', '399')
),

sqft_percentiles AS (
    SELECT
        ch.year,
        leg.user1 AS township_code,
        CAST(APPROX_PERCENTILE(ch.char_land_sf, 0.95) AS INT)
            AS char_land_sf_95_percentile
    FROM {{ ref('default.vw_pin_condo_char') }} AS ch
    LEFT JOIN {{ source('iasworld', 'legdat') }} AS leg
        ON ch.pin = leg.parid
        AND ch.year = leg.taxyr
        AND leg.cur = 'Y'
        AND leg.deactivat IS NULL
    GROUP BY ch.year, leg.user1
)

SELECT
    uni.*,

    CASE
        WHEN ch.is_parking_space = TRUE THEN 'NONLIVABLE'
        ELSE 'CONDO'
    END AS meta_modeling_group,

    -- Proration fields. Buildings can be split over multiple PINs, with each
    -- PIN owning a percentage of a building. For residential buildings, if
    -- a proration rate is NULL or 0, it's almost always actually 1
    ch.tieback_key_pin AS meta_tieback_key_pin,
    ch.tieback_proration_rate AS meta_tieback_proration_rate,
    COALESCE(ch.tieback_proration_rate < 1.0, FALSE) AS ind_pin_is_prorated,
    ch.card_proration_rate AS meta_card_proration_rate,

    -- Multicard/multi-landline related fields. Each PIN can have more than
    -- one improvement/card AND/OR more than one attached landline
    ch.card AS meta_card_num,
    ch.lline AS meta_lline_num,
    ch.pin_is_multilline AS ind_pin_is_multilline,
    ch.pin_num_lline AS meta_pin_num_lline,
    ch.pin_is_multiland AS ind_pin_is_multiland,
    ch.pin_num_landlines AS meta_pin_num_landlines,
    ch.cdu AS meta_cdu,

    -- Property characteristics from iasWorld
    ch.char_yrblt,
    NULLIF(ch.char_land_sf, 0.0) AS char_land_sf,
    ch.char_building_pins,
    ch.char_building_pins - ch.char_building_non_units AS char_building_units,
    ch.char_building_non_units,
    ch.bldg_is_mixed_use AS char_bldg_is_mixed_use,

    -- Property characteristics from MLS/valuations
    ch.char_building_sf,
    ch.char_unit_sf,
    ch.char_bedrooms,
    ch.char_half_baths,
    ch.char_full_baths,

    -- Land and lot size indicators
    sp.char_land_sf_95_percentile,
    COALESCE(
        ch.char_land_sf >= sp.char_land_sf_95_percentile,
        FALSE
    ) AS ind_land_gte_95_percentile,
    uni.meta_year AS year
FROM uni
LEFT JOIN {{ ref('default.vw_pin_condo_char') }} AS ch
    ON uni.meta_pin = ch.pin
    AND uni.meta_year = ch.year
LEFT JOIN sqft_percentiles AS sp
    ON uni.meta_year = sp.year
    AND uni.meta_township_code = sp.township_code
