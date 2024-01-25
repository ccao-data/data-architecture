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
    SELECT * FROM {{ ref('model.vw_pin_shared_input') }}
    WHERE meta_class IN ('299', '399')
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
    uni.meta_pin,
    uni.meta_pin10,
    uni.meta_year,
    uni.meta_class,
    uni.meta_triad_name,
    uni.meta_triad_code,
    uni.meta_township_name,
    uni.meta_township_code,
    uni.meta_nbhd_code,
    uni.meta_tax_code,
    uni.meta_mailed_bldg,
    uni.meta_mailed_land,
    uni.meta_mailed_tot,
    uni.meta_certified_bldg,
    uni.meta_certified_land,
    uni.meta_certified_tot,
    uni.meta_board_bldg,
    uni.meta_board_land,
    uni.meta_board_tot,
    uni.meta_1yr_pri_board_bldg,
    uni.meta_1yr_pri_board_land,
    uni.meta_1yr_pri_board_tot,
    uni.meta_2yr_pri_board_bldg,
    uni.meta_2yr_pri_board_land,
    uni.meta_2yr_pri_board_tot,
    uni.loc_property_address,
    uni.loc_property_city,
    uni.loc_property_state,
    uni.loc_property_zip,
    uni.loc_longitude,
    uni.loc_latitude,
    uni.loc_x_3435,
    uni.loc_y_3435,
    uni.loc_census_puma_geoid,
    uni.loc_census_tract_geoid,
    uni.loc_census_data_year,
    uni.loc_census_acs5_puma_geoid,
    uni.loc_census_acs5_tract_geoid,
    uni.loc_census_acs5_data_year,
    CASE
        WHEN
            uni.loc_tax_municipality_name IS NOT NULL
            THEN uni.loc_tax_municipality_name
        WHEN
            uni.loc_tax_municipality_name IS NULL
            THEN nn1.loc_tax_municipality_name
        WHEN
            nn1.loc_tax_municipality_name IS NULL
            THEN nn2.loc_tax_municipality_name
    END AS loc_tax_municipality_name,
    CASE
        WHEN uni.loc_ward_num IS NOT NULL THEN uni.loc_ward_num
        WHEN uni.loc_ward_num IS NULL THEN nn1.loc_ward_num
        WHEN nn1.loc_ward_num IS NULL THEN nn2.loc_ward_num
    END AS loc_ward_num,
    CASE
        WHEN
            uni.loc_chicago_community_area_name IS NOT NULL
            THEN uni.loc_chicago_community_area_name
        WHEN
            uni.loc_chicago_community_area_name IS NULL
            THEN nn1.loc_chicago_community_area_name
        WHEN
            nn1.loc_chicago_community_area_name IS NULL
            THEN nn2.loc_chicago_community_area_name
    END AS loc_chicago_community_area_name,
    CASE
        WHEN
            uni.loc_school_elementary_district_geoid IS NOT NULL
            THEN uni.loc_school_elementary_district_geoid
        WHEN
            uni.loc_school_elementary_district_geoid IS NULL
            THEN nn1.loc_school_elementary_district_geoid
        WHEN
            nn1.loc_school_elementary_district_geoid IS NULL
            THEN nn2.loc_school_elementary_district_geoid
    END AS loc_school_elementary_district_geoid,
    CASE
        WHEN
            uni.loc_school_secondary_district_geoid IS NOT NULL
            THEN uni.loc_school_secondary_district_geoid
        WHEN
            uni.loc_school_secondary_district_geoid IS NULL
            THEN nn1.loc_school_secondary_district_geoid
        WHEN
            nn1.loc_school_secondary_district_geoid IS NULL
            THEN nn2.loc_school_secondary_district_geoid
    END AS loc_school_secondary_district_geoid,
    CASE
        WHEN
            uni.loc_school_unified_district_geoid IS NOT NULL
            THEN uni.loc_school_unified_district_geoid
        WHEN
            uni.loc_school_unified_district_geoid IS NULL
            THEN nn1.loc_school_unified_district_geoid
        WHEN
            nn1.loc_school_unified_district_geoid IS NULL
            THEN nn2.loc_school_unified_district_geoid
    END AS loc_school_unified_district_geoid,
    CASE
        WHEN
            uni.loc_tax_special_service_area_num IS NOT NULL
            THEN uni.loc_tax_special_service_area_num
        WHEN
            uni.loc_tax_special_service_area_num IS NULL
            THEN nn1.loc_tax_special_service_area_num
        WHEN
            nn1.loc_tax_special_service_area_num IS NULL
            THEN nn2.loc_tax_special_service_area_num
    END AS loc_tax_special_service_area_num,
    CASE
        WHEN
            uni.loc_tax_tif_district_num IS NOT NULL
            THEN uni.loc_tax_tif_district_num
        WHEN
            uni.loc_tax_tif_district_num IS NULL
            THEN nn1.loc_tax_tif_district_num
        WHEN
            nn1.loc_tax_tif_district_num IS NULL
            THEN nn2.loc_tax_tif_district_num
    END AS loc_tax_tif_district_num,
    CASE
        WHEN
            uni.loc_misc_subdivision_id IS NOT NULL
            THEN uni.loc_misc_subdivision_id
        WHEN uni.loc_misc_subdivision_id IS NULL THEN nn1.loc_misc_subdivision_id
        WHEN
            nn1.loc_misc_subdivision_id IS NULL
            THEN nn2.loc_misc_subdivision_id
    END AS loc_misc_subdivision_id,
    CASE
        WHEN
            uni.loc_env_flood_fema_sfha IS NOT NULL
            THEN uni.loc_env_flood_fema_sfha
        WHEN uni.loc_env_flood_fema_sfha IS NULL THEN nn1.loc_env_flood_fema_sfha
        WHEN
            nn1.loc_env_flood_fema_sfha IS NULL
            THEN nn2.loc_env_flood_fema_sfha
    END AS loc_env_flood_fema_sfha,
    CASE
        WHEN
            uni.loc_env_flood_fs_factor IS NOT NULL
            THEN uni.loc_env_flood_fs_factor
        WHEN uni.loc_env_flood_fs_factor IS NULL THEN nn1.loc_env_flood_fs_factor
        WHEN
            nn1.loc_env_flood_fs_factor IS NULL
            THEN nn2.loc_env_flood_fs_factor
    END AS loc_env_flood_fs_factor,
    CASE
        WHEN
            uni.loc_env_flood_fs_risk_direction IS NOT NULL
            THEN uni.loc_env_flood_fs_risk_direction
        WHEN
            uni.loc_env_flood_fs_risk_direction IS NULL
            THEN nn1.loc_env_flood_fs_risk_direction
        WHEN
            nn1.loc_env_flood_fs_risk_direction IS NULL
            THEN nn2.loc_env_flood_fs_risk_direction
    END AS loc_env_flood_fs_risk_direction,
    CASE
        WHEN
            uni.loc_env_ohare_noise_contour_no_buffer_bool IS NOT NULL
            THEN uni.loc_env_ohare_noise_contour_no_buffer_bool
        WHEN
            uni.loc_env_ohare_noise_contour_no_buffer_bool IS NULL
            THEN nn1.loc_env_ohare_noise_contour_no_buffer_bool
        WHEN
            nn1.loc_env_ohare_noise_contour_no_buffer_bool IS NULL
            THEN nn2.loc_env_ohare_noise_contour_no_buffer_bool
    END AS loc_env_ohare_noise_contour_no_buffer_bool,
    CASE
        WHEN
            uni.loc_env_airport_noise_dnl IS NOT NULL
            THEN uni.loc_env_airport_noise_dnl
        WHEN
            uni.loc_env_airport_noise_dnl IS NULL
            THEN nn1.loc_env_airport_noise_dnl
        WHEN
            nn1.loc_env_airport_noise_dnl IS NULL
            THEN nn2.loc_env_airport_noise_dnl
    END AS loc_env_airport_noise_dnl,
    CASE
        WHEN
            uni.loc_access_cmap_walk_nta_score IS NOT NULL
            THEN uni.loc_access_cmap_walk_nta_score
        WHEN
            uni.loc_access_cmap_walk_nta_score IS NULL
            THEN nn1.loc_access_cmap_walk_nta_score
        WHEN
            nn1.loc_access_cmap_walk_nta_score IS NULL
            THEN nn2.loc_access_cmap_walk_nta_score
    END AS loc_access_cmap_walk_nta_score,
    CASE
        WHEN
            uni.loc_access_cmap_walk_total_score IS NOT NULL
            THEN uni.loc_access_cmap_walk_total_score
        WHEN
            uni.loc_access_cmap_walk_total_score IS NULL
            THEN nn1.loc_access_cmap_walk_total_score
        WHEN
            nn1.loc_access_cmap_walk_total_score IS NULL
            THEN nn2.loc_access_cmap_walk_total_score
    END AS loc_access_cmap_walk_total_score,
    uni.prox_num_pin_in_half_mile,
    uni.prox_num_bus_stop_in_half_mile,
    uni.prox_num_foreclosure_per_1000_pin_past_5_years,
    CASE
        WHEN
            uni.prox_num_school_in_half_mile IS NOT NULL
            THEN uni.prox_num_school_in_half_mile
        WHEN
            uni.prox_num_school_in_half_mile IS NULL
            THEN nn1.prox_num_school_in_half_mile
        WHEN
            nn1.prox_num_school_in_half_mile IS NULL
            THEN nn2.prox_num_school_in_half_mile
    END AS prox_num_school_in_half_mile,
    CASE
        WHEN
            uni.prox_num_school_with_rating_in_half_mile IS NOT NULL
            THEN uni.prox_num_school_with_rating_in_half_mile
        WHEN
            uni.prox_num_school_with_rating_in_half_mile IS NULL
            THEN nn1.prox_num_school_with_rating_in_half_mile
        WHEN
            nn1.prox_num_school_with_rating_in_half_mile IS NULL
            THEN nn2.prox_num_school_with_rating_in_half_mile
    END AS prox_num_school_with_rating_in_half_mile,
    CASE
        WHEN
            uni.prox_avg_school_rating_in_half_mile IS NOT NULL
            THEN uni.prox_avg_school_rating_in_half_mile
        WHEN
            uni.prox_avg_school_rating_in_half_mile IS NULL
            THEN nn1.prox_avg_school_rating_in_half_mile
        WHEN
            nn1.prox_avg_school_rating_in_half_mile IS NULL
            THEN nn2.prox_avg_school_rating_in_half_mile
    END AS prox_avg_school_rating_in_half_mile,
    CASE
        WHEN
            uni.prox_airport_dnl_total IS NOT NULL
            THEN uni.prox_airport_dnl_total
        WHEN
            uni.prox_airport_dnl_total IS NULL
            THEN nn1.prox_airport_dnl_total
        WHEN
            nn1.prox_airport_dnl_total IS NULL
            THEN nn2.prox_airport_dnl_total
    END AS prox_airport_dnl_total,
    CASE
        WHEN
            uni.prox_nearest_bike_trail_dist_ft IS NOT NULL
            THEN uni.prox_nearest_bike_trail_dist_ft
        WHEN
            uni.prox_nearest_bike_trail_dist_ft IS NULL
            THEN nn1.prox_nearest_bike_trail_dist_ft
        WHEN
            nn1.prox_nearest_bike_trail_dist_ft IS NULL
            THEN nn2.prox_nearest_bike_trail_dist_ft
    END AS prox_nearest_bike_trail_dist_ft,
    CASE
        WHEN
            uni.prox_nearest_cemetery_dist_ft IS NOT NULL
            THEN uni.prox_nearest_cemetery_dist_ft
        WHEN
            uni.prox_nearest_cemetery_dist_ft IS NULL
            THEN nn1.prox_nearest_cemetery_dist_ft
        WHEN
            nn1.prox_nearest_cemetery_dist_ft IS NULL
            THEN nn2.prox_nearest_cemetery_dist_ft
    END AS prox_nearest_cemetery_dist_ft,
    uni.prox_nearest_cta_route_dist_ft,
    uni.prox_nearest_cta_stop_dist_ft,
    uni.prox_nearest_golf_course_dist_ft,
    CASE
        WHEN
            uni.prox_nearest_hospital_dist_ft IS NOT NULL
            THEN uni.prox_nearest_hospital_dist_ft
        WHEN
            uni.prox_nearest_hospital_dist_ft IS NULL
            THEN nn1.prox_nearest_hospital_dist_ft
        WHEN
            nn1.prox_nearest_hospital_dist_ft IS NULL
            THEN nn2.prox_nearest_hospital_dist_ft
    END AS prox_nearest_hospital_dist_ft,
    CASE
        WHEN
            uni.prox_lake_michigan_dist_ft IS NOT NULL
            THEN uni.prox_lake_michigan_dist_ft
        WHEN
            uni.prox_lake_michigan_dist_ft IS NULL
            THEN nn1.prox_lake_michigan_dist_ft
        WHEN
            nn1.prox_lake_michigan_dist_ft IS NULL
            THEN nn2.prox_lake_michigan_dist_ft
    END AS prox_lake_michigan_dist_ft,
    CASE
        WHEN
            uni.prox_nearest_major_road_dist_ft IS NOT NULL
            THEN uni.prox_nearest_major_road_dist_ft
        WHEN
            uni.prox_nearest_major_road_dist_ft IS NULL
            THEN nn1.prox_nearest_major_road_dist_ft
        WHEN
            nn1.prox_nearest_major_road_dist_ft IS NULL
            THEN nn2.prox_nearest_major_road_dist_ft
    END AS prox_nearest_major_road_dist_ft,
    uni.prox_nearest_metra_route_dist_ft,
    uni.prox_nearest_metra_stop_dist_ft,
    CASE
        WHEN
            uni.prox_nearest_park_dist_ft IS NOT NULL
            THEN uni.prox_nearest_park_dist_ft
        WHEN
            uni.prox_nearest_park_dist_ft IS NULL
            THEN nn1.prox_nearest_park_dist_ft
        WHEN
            nn1.prox_nearest_park_dist_ft IS NULL
            THEN nn2.prox_nearest_park_dist_ft
    END AS prox_nearest_park_dist_ft,
    CASE
        WHEN
            uni.prox_nearest_railroad_dist_ft IS NOT NULL
            THEN uni.prox_nearest_railroad_dist_ft
        WHEN
            uni.prox_nearest_railroad_dist_ft IS NULL
            THEN nn1.prox_nearest_railroad_dist_ft
        WHEN
            nn1.prox_nearest_railroad_dist_ft IS NULL
            THEN nn2.prox_nearest_railroad_dist_ft
    END AS prox_nearest_railroad_dist_ft,
    CASE
        WHEN
            uni.prox_nearest_secondary_road_dist_ft IS NOT NULL
            THEN uni.prox_nearest_secondary_road_dist_ft
        WHEN
            uni.prox_nearest_secondary_road_dist_ft IS NULL
            THEN nn1.prox_nearest_secondary_road_dist_ft
        WHEN
            nn1.prox_nearest_secondary_road_dist_ft IS NULL
            THEN nn2.prox_nearest_secondary_road_dist_ft
    END AS prox_nearest_secondary_road_dist_ft,
    CASE
        WHEN
            uni.prox_nearest_university_dist_ft IS NOT NULL
            THEN uni.prox_nearest_university_dist_ft
        WHEN
            uni.prox_nearest_university_dist_ft IS NULL
            THEN nn1.prox_nearest_university_dist_ft
        WHEN
            nn1.prox_nearest_university_dist_ft IS NULL
            THEN nn2.prox_nearest_university_dist_ft
    END AS prox_nearest_university_dist_ft,
    CASE
        WHEN
            uni.prox_nearest_vacant_land_dist_ft IS NOT NULL
            THEN uni.prox_nearest_vacant_land_dist_ft
        WHEN
            uni.prox_nearest_vacant_land_dist_ft IS NULL
            THEN nn1.prox_nearest_vacant_land_dist_ft
        WHEN
            nn1.prox_nearest_vacant_land_dist_ft IS NULL
            THEN nn2.prox_nearest_vacant_land_dist_ft
    END AS prox_nearest_vacant_land_dist_ft,
    CASE
        WHEN
            uni.prox_nearest_water_dist_ft IS NOT NULL
            THEN uni.prox_nearest_water_dist_ft
        WHEN
            uni.prox_nearest_water_dist_ft IS NULL
            THEN nn1.prox_nearest_water_dist_ft
        WHEN
            nn1.prox_nearest_water_dist_ft IS NULL
            THEN nn2.prox_nearest_water_dist_ft
    END AS prox_nearest_water_dist_ft,
    uni.acs5_count_sex_total,
    uni.acs5_percent_age_children,
    uni.acs5_percent_age_senior,
    uni.acs5_median_age_total,
    uni.acs5_percent_mobility_no_move,
    uni.acs5_percent_mobility_moved_in_county,
    uni.acs5_percent_mobility_moved_from_other_state,
    uni.acs5_percent_household_family_married,
    uni.acs5_percent_household_nonfamily_alone,
    uni.acs5_percent_education_high_school,
    uni.acs5_percent_education_bachelor,
    uni.acs5_percent_education_graduate,
    uni.acs5_percent_income_below_poverty_level,
    uni.acs5_median_income_household_past_year,
    uni.acs5_median_income_per_capita_past_year,
    uni.acs5_percent_income_household_received_snap_past_year,
    uni.acs5_percent_employment_unemployed,
    uni.acs5_median_household_total_occupied_year_built,
    uni.acs5_median_household_renter_occupied_gross_rent,
    uni.acs5_median_household_owner_occupied_value,
    uni.acs5_percent_household_owner_occupied,
    uni.acs5_percent_household_total_occupied_w_sel_cond,
    uni.other_ihs_avg_year_index,
    uni.other_tax_bill_amount_total,
    uni.other_tax_bill_rate,
    CASE
        WHEN
            uni.other_school_district_elementary_avg_rating IS NOT NULL
            THEN uni.other_school_district_elementary_avg_rating
        WHEN
            uni.other_school_district_elementary_avg_rating IS NULL
            THEN nn1.other_school_district_elementary_avg_rating
        WHEN
            nn1.other_school_district_elementary_avg_rating IS NULL
            THEN nn2.other_school_district_elementary_avg_rating
    END AS other_school_district_elementary_avg_rating,
    CASE
        WHEN
            uni.other_school_district_secondary_avg_rating IS NOT NULL
            THEN uni.other_school_district_secondary_avg_rating
        WHEN
            uni.other_school_district_secondary_avg_rating IS NULL
            THEN nn1.other_school_district_secondary_avg_rating
        WHEN
            nn1.other_school_district_secondary_avg_rating IS NULL
            THEN nn2.other_school_district_secondary_avg_rating
    END AS other_school_district_secondary_avg_rating,
    -- Exemptions data is usually missing for the 1 or 2 years prior
    -- to the lien date, so we need to fill missing values w/ down up fill
    -- This assumes that people currently receiving exemptions keep them
    COALESCE(
        uni.ccao_is_active_exe_homeowner,
        LAG(uni.ccao_is_active_exe_homeowner)
            IGNORE NULLS
            OVER (PARTITION BY uni.meta_pin ORDER BY uni.meta_year)
    ) AS ccao_is_active_exe_homeowner,
    COALESCE(
        uni.ccao_n_years_exe_homeowner,
        LAG(uni.ccao_n_years_exe_homeowner)
            IGNORE NULLS
            OVER (PARTITION BY uni.meta_pin ORDER BY uni.meta_year)
    ) AS ccao_n_years_exe_homeowner,
    uni.ccao_is_corner_lot,

    CASE
        WHEN ch.is_parking_space = TRUE
            OR ch.is_common_area = TRUE THEN 'NONLIVABLE'
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
LEFT JOIN default.vw_pin_condo_char AS ch
    ON uni.meta_pin = ch.pin
    AND uni.meta_year = ch.year
LEFT JOIN sqft_percentiles AS sp
    ON uni.meta_year = sp.year
    AND uni.meta_township_code = sp.township_code
LEFT JOIN (
    SELECT * FROM (
        SELECT
        uni.meta_pin10,
        uni.meta_year,
        uni.loc_tax_municipality_name,
        uni.loc_ward_num,
        uni.loc_chicago_community_area_name,
        uni.loc_school_elementary_district_geoid,
        uni.loc_school_secondary_district_geoid,
        uni.loc_school_unified_district_geoid,
        uni.loc_tax_special_service_area_num,
        uni.loc_tax_tif_district_num,
        uni.loc_misc_subdivision_id,
        uni.loc_env_flood_fema_sfha,
        uni.loc_env_flood_fs_factor,
        uni.loc_env_flood_fs_risk_direction,
        uni.loc_env_ohare_noise_contour_no_buffer_bool,
        uni.loc_env_airport_noise_dnl,
        uni.loc_access_cmap_walk_nta_score,
        uni.loc_access_cmap_walk_total_score,
        uni.prox_num_pin_in_half_mile,
        uni.prox_num_bus_stop_in_half_mile,
        uni.prox_num_foreclosure_per_1000_pin_past_5_years,
        uni.prox_num_school_in_half_mile,
        uni.prox_num_school_with_rating_in_half_mile,
        uni.prox_avg_school_rating_in_half_mile,
        uni.prox_airport_dnl_total,
        uni.prox_nearest_bike_trail_dist_ft,
        uni.prox_nearest_cemetery_dist_ft,
        uni.prox_nearest_hospital_dist_ft,
        uni.prox_lake_michigan_dist_ft,
        uni.prox_nearest_major_road_dist_ft,
        uni.prox_nearest_metra_route_dist_ft,
        uni.prox_nearest_metra_stop_dist_ft,
        uni.prox_nearest_park_dist_ft,
        uni.prox_nearest_railroad_dist_ft,
        uni.prox_nearest_secondary_road_dist_ft,
        uni.prox_nearest_university_dist_ft,
        uni.prox_nearest_vacant_land_dist_ft,
        uni.prox_nearest_water_dist_ft,
        uni.other_school_district_elementary_avg_rating,
        uni.other_school_district_secondary_avg_rating,
        ROW_NUMBER() OVER (PARTITION BY
            meta_pin10, meta_year) AS row
    FROM uni
    WHERE nearest_neighbor_1_dist_ft <= 500
    ) AS nn1
    WHERE row = 1

) AS nn1
    ON uni.nearest_neighbor_1_pin10 = nn1.meta_pin10
    AND uni.meta_year = nn1.meta_year
LEFT JOIN (
    SELECT * FROM (
        SELECT DISTINCT
            uni.meta_pin10,
            uni.meta_year,
            uni.loc_tax_municipality_name,
            uni.loc_ward_num,
            uni.loc_chicago_community_area_name,
            uni.loc_school_elementary_district_geoid,
            uni.loc_school_secondary_district_geoid,
            uni.loc_school_unified_district_geoid,
            uni.loc_tax_special_service_area_num,
            uni.loc_tax_tif_district_num,
            uni.loc_misc_subdivision_id,
            uni.loc_env_flood_fema_sfha,
            uni.loc_env_flood_fs_factor,
            uni.loc_env_flood_fs_risk_direction,
            uni.loc_env_ohare_noise_contour_no_buffer_bool,
            uni.loc_env_airport_noise_dnl,
            uni.loc_access_cmap_walk_nta_score,
            uni.loc_access_cmap_walk_total_score,
            uni.prox_num_pin_in_half_mile,
            uni.prox_num_bus_stop_in_half_mile,
            uni.prox_num_foreclosure_per_1000_pin_past_5_years,
            uni.prox_num_school_in_half_mile,
            uni.prox_num_school_with_rating_in_half_mile,
            uni.prox_avg_school_rating_in_half_mile,
            uni.prox_airport_dnl_total,
            uni.prox_nearest_bike_trail_dist_ft,
            uni.prox_nearest_cemetery_dist_ft,
            uni.prox_nearest_hospital_dist_ft,
            uni.prox_lake_michigan_dist_ft,
            uni.prox_nearest_major_road_dist_ft,
            uni.prox_nearest_metra_route_dist_ft,
            uni.prox_nearest_metra_stop_dist_ft,
            uni.prox_nearest_park_dist_ft,
            uni.prox_nearest_railroad_dist_ft,
            uni.prox_nearest_secondary_road_dist_ft,
            uni.prox_nearest_university_dist_ft,
            uni.prox_nearest_vacant_land_dist_ft,
            uni.prox_nearest_water_dist_ft,
            uni.other_school_district_elementary_avg_rating,
            uni.other_school_district_secondary_avg_rating,
        ROW_NUMBER() OVER (PARTITION BY
            meta_pin10, meta_year) AS row
        FROM uni
        WHERE nearest_neighbor_2_dist_ft <= 500
    ) AS nn2
    WHERE row = 1
) AS nn2
    ON uni.nearest_neighbor_1_pin10 = nn2.meta_pin10
    AND uni.meta_year = nn2.meta_year
