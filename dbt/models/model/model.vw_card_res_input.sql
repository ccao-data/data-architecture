/*
View containing cleaned, filled data for residential modeling. Missing data is
filled with the following steps:

1. All historical data is filled FORWARD in time, i.e. data from 2020 fills
   2021 as long as the data isn't something which frequently changes
2. Current data is filled BACKWARD to account for missing historical data.
   Again, this is only for things unlikely to change
3. If any gaps remain, data is filled with the data of the nearest neighboring
   PIN (ONLY for things that don't vary at the property level, such as census
   tract statistics. Property characteristics are NOT filled)

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
    WHERE meta_class IN (
            '202', '203', '204', '205', '206', '207', '208', '209',
            '210', '211', '212', '218', '219', '234', '278', '295'
        )
),

sqft_percentiles AS (
    SELECT
        ch.year,
        leg.user1 AS township_code,
        CAST(APPROX_PERCENTILE(ch.char_bldg_sf, 0.95) AS INT)
            AS char_bldg_sf_95_percentile,
        CAST(APPROX_PERCENTILE(ch.char_land_sf, 0.95) AS INT)
            AS char_land_sf_95_percentile
    FROM {{ ref('default.vw_card_res_char') }} AS ch
    LEFT JOIN {{ source('iasworld', 'legdat') }} AS leg
        ON ch.pin = leg.parid
        AND ch.year = leg.taxyr
        AND leg.cur = 'Y'
        AND leg.deactivat IS NULL
    GROUP BY ch.year, leg.user1
),

forward_fill AS (
    SELECT
        uni.*,

        CASE
            WHEN uni.meta_class IN ('211', '212') THEN 'MF'
            WHEN uni.meta_class IN ('218', '219') THEN 'BB'
            ELSE 'SF'
        END AS meta_modeling_group,

        -- Proration fields. Buildings can be split over multiple PINs, with
        -- each PIN owning a percentage of a building. For residential
        -- buildings, if a proration rate is NULL or 0, it's almost always
        -- actually 1
        ch.tieback_key_pin AS meta_tieback_key_pin,
        CASE
            WHEN ch.tieback_proration_rate IS NULL THEN 1.0
            WHEN ch.tieback_proration_rate = 0.0 THEN 1.0
            ELSE ch.tieback_proration_rate
        END AS meta_tieback_proration_rate,
        COALESCE(
            ch.tieback_proration_rate < 1.0,
            FALSE
        ) AS ind_pin_is_prorated,
        ch.card_proration_rate AS meta_card_proration_rate,

        -- Multicard/multi-landline related fields. Each PIN can have more than
        -- one improvement/card AND/OR more than one attached landline
        ch.card AS meta_card_num,
        ch.pin_is_multicard AS ind_pin_is_multicard,
        ch.pin_num_cards AS meta_pin_num_cards,
        ch.pin_is_multiland AS ind_pin_is_multiland,
        ch.pin_num_landlines AS meta_pin_num_landlines,
        ch.cdu AS meta_cdu,

        -- Property characteristics from iasWorld
        ch.char_yrblt,
        NULLIF(ch.char_bldg_sf, 0.0) AS char_bldg_sf,
        NULLIF(ch.char_land_sf, 0.0) AS char_land_sf,
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
        ch.char_class,
        ch.char_use,
        ch.char_porch,
        ch.char_air,
        ch.char_ncu,
        ch.char_tp_plan,
        ch.char_recent_renovation,

        -- Land and lot size indicators
        sp.char_land_sf_95_percentile,
        COALESCE(
            ch.char_land_sf >= sp.char_land_sf_95_percentile,
            FALSE
        ) AS ind_land_gte_95_percentile,
        sp.char_bldg_sf_95_percentile,
        COALESCE(
            ch.char_bldg_sf >= sp.char_bldg_sf_95_percentile,
            FALSE
        ) AS ind_bldg_gte_95_percentile,
        COALESCE(
            (ch.char_land_sf / (ch.char_bldg_sf + 1)) >= 10.0,
            FALSE
        ) AS ind_land_bldg_ratio_gte_10

    FROM uni
    LEFT JOIN {{ ref('default.vw_card_res_char') }} AS ch
        ON uni.meta_pin = ch.pin
        AND uni.meta_year = ch.year
    LEFT JOIN sqft_percentiles AS sp
        ON uni.meta_year = sp.year
        AND uni.meta_township_code = sp.township_code
)

-- Anything with a CASE WHEN here is just borrowing missing values
-- from its nearest spatial neighbor
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
    f1.loc_x_3435,
    f1.loc_y_3435,
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
    f1.char_class,
    f1.char_use,
    f1.char_porch,
    f1.char_air,
    f1.char_ncu,
    f1.char_tp_plan,
    f1.char_recent_renovation,
    f1.char_land_sf_95_percentile,
    f1.ind_land_gte_95_percentile,
    f1.char_bldg_sf_95_percentile,
    f1.ind_bldg_gte_95_percentile,
    f1.ind_land_bldg_ratio_gte_10,
    f1.loc_census_puma_geoid,
    f1.loc_census_tract_geoid,
    f1.loc_census_data_year,
    f1.loc_census_acs5_puma_geoid,
    f1.loc_census_acs5_tract_geoid,
    f1.loc_census_acs5_data_year,
    CASE
        WHEN
            f1.loc_tax_municipality_name IS NOT NULL
            THEN f1.loc_tax_municipality_name
        WHEN
            f1.loc_tax_municipality_name IS NULL
            THEN nn1.loc_tax_municipality_name
        WHEN
            nn1.loc_tax_municipality_name IS NULL
            THEN nn2.loc_tax_municipality_name
    END AS loc_tax_municipality_name,
    CASE
        WHEN f1.loc_ward_num IS NOT NULL THEN f1.loc_ward_num
        WHEN f1.loc_ward_num IS NULL THEN nn1.loc_ward_num
        WHEN nn1.loc_ward_num IS NULL THEN nn2.loc_ward_num
    END AS loc_ward_num,
    CASE
        WHEN
            f1.loc_chicago_community_area_name IS NOT NULL
            THEN f1.loc_chicago_community_area_name
        WHEN
            f1.loc_chicago_community_area_name IS NULL
            THEN nn1.loc_chicago_community_area_name
        WHEN
            nn1.loc_chicago_community_area_name IS NULL
            THEN nn2.loc_chicago_community_area_name
    END AS loc_chicago_community_area_name,
    CASE
        WHEN
            f1.loc_school_elementary_district_geoid IS NOT NULL
            THEN f1.loc_school_elementary_district_geoid
        WHEN
            f1.loc_school_elementary_district_geoid IS NULL
            THEN nn1.loc_school_elementary_district_geoid
        WHEN
            nn1.loc_school_elementary_district_geoid IS NULL
            THEN nn2.loc_school_elementary_district_geoid
    END AS loc_school_elementary_district_geoid,
    CASE
        WHEN
            f1.loc_school_secondary_district_geoid IS NOT NULL
            THEN f1.loc_school_secondary_district_geoid
        WHEN
            f1.loc_school_secondary_district_geoid IS NULL
            THEN nn1.loc_school_secondary_district_geoid
        WHEN
            nn1.loc_school_secondary_district_geoid IS NULL
            THEN nn2.loc_school_secondary_district_geoid
    END AS loc_school_secondary_district_geoid,
    CASE
        WHEN
            f1.loc_school_unified_district_geoid IS NOT NULL
            THEN f1.loc_school_unified_district_geoid
        WHEN
            f1.loc_school_unified_district_geoid IS NULL
            THEN nn1.loc_school_unified_district_geoid
        WHEN
            nn1.loc_school_unified_district_geoid IS NULL
            THEN nn2.loc_school_unified_district_geoid
    END AS loc_school_unified_district_geoid,
    CASE
        WHEN
            f1.loc_tax_special_service_area_num IS NOT NULL
            THEN f1.loc_tax_special_service_area_num
        WHEN
            f1.loc_tax_special_service_area_num IS NULL
            THEN nn1.loc_tax_special_service_area_num
        WHEN
            nn1.loc_tax_special_service_area_num IS NULL
            THEN nn2.loc_tax_special_service_area_num
    END AS loc_tax_special_service_area_num,
    CASE
        WHEN
            f1.loc_tax_tif_district_num IS NOT NULL
            THEN f1.loc_tax_tif_district_num
        WHEN
            f1.loc_tax_tif_district_num IS NULL
            THEN nn1.loc_tax_tif_district_num
        WHEN
            nn1.loc_tax_tif_district_num IS NULL
            THEN nn2.loc_tax_tif_district_num
    END AS loc_tax_tif_district_num,
    CASE
        WHEN
            f1.loc_misc_subdivision_id IS NOT NULL
            THEN f1.loc_misc_subdivision_id
        WHEN f1.loc_misc_subdivision_id IS NULL THEN nn1.loc_misc_subdivision_id
        WHEN
            nn1.loc_misc_subdivision_id IS NULL
            THEN nn2.loc_misc_subdivision_id
    END AS loc_misc_subdivision_id,
    CASE
        WHEN
            f1.loc_env_flood_fema_sfha IS NOT NULL
            THEN f1.loc_env_flood_fema_sfha
        WHEN f1.loc_env_flood_fema_sfha IS NULL THEN nn1.loc_env_flood_fema_sfha
        WHEN
            nn1.loc_env_flood_fema_sfha IS NULL
            THEN nn2.loc_env_flood_fema_sfha
    END AS loc_env_flood_fema_sfha,
    CASE
        WHEN
            f1.loc_env_flood_fs_factor IS NOT NULL
            THEN f1.loc_env_flood_fs_factor
        WHEN f1.loc_env_flood_fs_factor IS NULL THEN nn1.loc_env_flood_fs_factor
        WHEN
            nn1.loc_env_flood_fs_factor IS NULL
            THEN nn2.loc_env_flood_fs_factor
    END AS loc_env_flood_fs_factor,
    CASE
        WHEN
            f1.loc_env_flood_fs_risk_direction IS NOT NULL
            THEN f1.loc_env_flood_fs_risk_direction
        WHEN
            f1.loc_env_flood_fs_risk_direction IS NULL
            THEN nn1.loc_env_flood_fs_risk_direction
        WHEN
            nn1.loc_env_flood_fs_risk_direction IS NULL
            THEN nn2.loc_env_flood_fs_risk_direction
    END AS loc_env_flood_fs_risk_direction,
    CASE
        WHEN
            f1.loc_env_ohare_noise_contour_no_buffer_bool IS NOT NULL
            THEN f1.loc_env_ohare_noise_contour_no_buffer_bool
        WHEN
            f1.loc_env_ohare_noise_contour_no_buffer_bool IS NULL
            THEN nn1.loc_env_ohare_noise_contour_no_buffer_bool
        WHEN
            nn1.loc_env_ohare_noise_contour_no_buffer_bool IS NULL
            THEN nn2.loc_env_ohare_noise_contour_no_buffer_bool
    END AS loc_env_ohare_noise_contour_no_buffer_bool,
    CASE
        WHEN
            f1.loc_env_airport_noise_dnl IS NOT NULL
            THEN f1.loc_env_airport_noise_dnl
        WHEN
            f1.loc_env_airport_noise_dnl IS NULL
            THEN nn1.loc_env_airport_noise_dnl
        WHEN
            nn1.loc_env_airport_noise_dnl IS NULL
            THEN nn2.loc_env_airport_noise_dnl
    END AS loc_env_airport_noise_dnl,
    CASE
        WHEN
            f1.loc_access_cmap_walk_nta_score IS NOT NULL
            THEN f1.loc_access_cmap_walk_nta_score
        WHEN
            f1.loc_access_cmap_walk_nta_score IS NULL
            THEN nn1.loc_access_cmap_walk_nta_score
        WHEN
            nn1.loc_access_cmap_walk_nta_score IS NULL
            THEN nn2.loc_access_cmap_walk_nta_score
    END AS loc_access_cmap_walk_nta_score,
    CASE
        WHEN
            f1.loc_access_cmap_walk_total_score IS NOT NULL
            THEN f1.loc_access_cmap_walk_total_score
        WHEN
            f1.loc_access_cmap_walk_total_score IS NULL
            THEN nn1.loc_access_cmap_walk_total_score
        WHEN
            nn1.loc_access_cmap_walk_total_score IS NULL
            THEN nn2.loc_access_cmap_walk_total_score
    END AS loc_access_cmap_walk_total_score,
    f1.prox_num_pin_in_half_mile,
    f1.prox_num_bus_stop_in_half_mile,
    f1.prox_num_foreclosure_per_1000_pin_past_5_years,
    CASE
        WHEN
            f1.prox_num_school_in_half_mile IS NOT NULL
            THEN f1.prox_num_school_in_half_mile
        WHEN
            f1.prox_num_school_in_half_mile IS NULL
            THEN nn1.prox_num_school_in_half_mile
        WHEN
            nn1.prox_num_school_in_half_mile IS NULL
            THEN nn2.prox_num_school_in_half_mile
    END AS prox_num_school_in_half_mile,
    CASE
        WHEN
            f1.prox_num_school_with_rating_in_half_mile IS NOT NULL
            THEN f1.prox_num_school_with_rating_in_half_mile
        WHEN
            f1.prox_num_school_with_rating_in_half_mile IS NULL
            THEN nn1.prox_num_school_with_rating_in_half_mile
        WHEN
            nn1.prox_num_school_with_rating_in_half_mile IS NULL
            THEN nn2.prox_num_school_with_rating_in_half_mile
    END AS prox_num_school_with_rating_in_half_mile,
    CASE
        WHEN
            f1.prox_avg_school_rating_in_half_mile IS NOT NULL
            THEN f1.prox_avg_school_rating_in_half_mile
        WHEN
            f1.prox_avg_school_rating_in_half_mile IS NULL
            THEN nn1.prox_avg_school_rating_in_half_mile
        WHEN
            nn1.prox_avg_school_rating_in_half_mile IS NULL
            THEN nn2.prox_avg_school_rating_in_half_mile
    END AS prox_avg_school_rating_in_half_mile,
    CASE
        WHEN
            f1.prox_airport_dnl_total IS NOT NULL
            THEN f1.prox_airport_dnl_total
        WHEN
            f1.prox_airport_dnl_total IS NULL
            THEN nn1.prox_airport_dnl_total
        WHEN
            nn1.prox_airport_dnl_total IS NULL
            THEN nn2.prox_airport_dnl_total
    END AS prox_airport_dnl_total,
    CASE
        WHEN
            f1.prox_nearest_bike_trail_dist_ft IS NOT NULL
            THEN f1.prox_nearest_bike_trail_dist_ft
        WHEN
            f1.prox_nearest_bike_trail_dist_ft IS NULL
            THEN nn1.prox_nearest_bike_trail_dist_ft
        WHEN
            nn1.prox_nearest_bike_trail_dist_ft IS NULL
            THEN nn2.prox_nearest_bike_trail_dist_ft
    END AS prox_nearest_bike_trail_dist_ft,
    CASE
        WHEN
            f1.prox_nearest_cemetery_dist_ft IS NOT NULL
            THEN f1.prox_nearest_cemetery_dist_ft
        WHEN
            f1.prox_nearest_cemetery_dist_ft IS NULL
            THEN nn1.prox_nearest_cemetery_dist_ft
        WHEN
            nn1.prox_nearest_cemetery_dist_ft IS NULL
            THEN nn2.prox_nearest_cemetery_dist_ft
    END AS prox_nearest_cemetery_dist_ft,
    f1.prox_nearest_cta_route_dist_ft,
    f1.prox_nearest_cta_stop_dist_ft,
    f1.prox_nearest_golf_course_dist_ft,
    f1.prox_nearest_grocery_store_dist_ft,
    CASE
        WHEN
            f1.prox_nearest_hospital_dist_ft IS NOT NULL
            THEN f1.prox_nearest_hospital_dist_ft
        WHEN
            f1.prox_nearest_hospital_dist_ft IS NULL
            THEN nn1.prox_nearest_hospital_dist_ft
        WHEN
            nn1.prox_nearest_hospital_dist_ft IS NULL
            THEN nn2.prox_nearest_hospital_dist_ft
    END AS prox_nearest_hospital_dist_ft,
    CASE
        WHEN
            f1.prox_lake_michigan_dist_ft IS NOT NULL
            THEN f1.prox_lake_michigan_dist_ft
        WHEN
            f1.prox_lake_michigan_dist_ft IS NULL
            THEN nn1.prox_lake_michigan_dist_ft
        WHEN
            nn1.prox_lake_michigan_dist_ft IS NULL
            THEN nn2.prox_lake_michigan_dist_ft
    END AS prox_lake_michigan_dist_ft,
    CASE
        WHEN
            f1.prox_nearest_major_road_dist_ft IS NOT NULL
            THEN f1.prox_nearest_major_road_dist_ft
        WHEN
            f1.prox_nearest_major_road_dist_ft IS NULL
            THEN nn1.prox_nearest_major_road_dist_ft
        WHEN
            nn1.prox_nearest_major_road_dist_ft IS NULL
            THEN nn2.prox_nearest_major_road_dist_ft
    END AS prox_nearest_major_road_dist_ft,
    f1.prox_nearest_metra_route_dist_ft,
    f1.prox_nearest_metra_stop_dist_ft,
    f1.prox_nearest_new_construction_dist_ft,
    CASE
        WHEN
            f1.prox_nearest_park_dist_ft IS NOT NULL
            THEN f1.prox_nearest_park_dist_ft
        WHEN
            f1.prox_nearest_park_dist_ft IS NULL
            THEN nn1.prox_nearest_park_dist_ft
        WHEN
            nn1.prox_nearest_park_dist_ft IS NULL
            THEN nn2.prox_nearest_park_dist_ft
    END AS prox_nearest_park_dist_ft,
    CASE
        WHEN
            f1.prox_nearest_railroad_dist_ft IS NOT NULL
            THEN f1.prox_nearest_railroad_dist_ft
        WHEN
            f1.prox_nearest_railroad_dist_ft IS NULL
            THEN nn1.prox_nearest_railroad_dist_ft
        WHEN
            nn1.prox_nearest_railroad_dist_ft IS NULL
            THEN nn2.prox_nearest_railroad_dist_ft
    END AS prox_nearest_railroad_dist_ft,
    CASE
        WHEN
            f1.prox_nearest_road_arterial_daily_traffic IS NOT NULL
            THEN f1.prox_nearest_road_arterial_daily_traffic
        WHEN
            f1.prox_nearest_road_arterial_daily_traffic IS NULL
            THEN nn1.prox_nearest_road_arterial_daily_traffic
        WHEN
            nn1.prox_nearest_road_arterial_daily_traffic IS NULL
            THEN nn2.prox_nearest_road_arterial_daily_traffic
    END AS prox_nearest_road_arterial_daily_traffic,
    CASE
        WHEN
            f1.prox_nearest_road_arterial_dist_ft IS NOT NULL
            THEN f1.prox_nearest_road_arterial_dist_ft
        WHEN
            f1.prox_nearest_road_arterial_dist_ft IS NULL
            THEN nn1.prox_nearest_road_arterial_dist_ft
        WHEN
            nn1.prox_nearest_road_arterial_dist_ft IS NULL
            THEN nn2.prox_nearest_road_arterial_dist_ft
    END AS prox_nearest_road_arterial_dist_ft,
    CASE
        WHEN
            f1.prox_nearest_road_arterial_lanes IS NOT NULL
            THEN f1.prox_nearest_road_arterial_lanes
        WHEN
            f1.prox_nearest_road_arterial_lanes IS NULL
            THEN nn1.prox_nearest_road_arterial_lanes
        WHEN
            nn1.prox_nearest_road_arterial_lanes IS NULL
            THEN nn2.prox_nearest_road_arterial_lanes
    END AS prox_nearest_road_arterial_lanes,
    CASE
        WHEN
            f1.prox_nearest_road_arterial_speed_limit IS NOT NULL
            THEN f1.prox_nearest_road_arterial_speed_limit
        WHEN
            f1.prox_nearest_road_arterial_speed_limit IS NULL
            THEN nn1.prox_nearest_road_arterial_speed_limit
        WHEN
            nn1.prox_nearest_road_arterial_speed_limit IS NULL
            THEN nn2.prox_nearest_road_arterial_speed_limit
    END AS prox_nearest_road_arterial_speed_limit,
    CASE
        WHEN
            f1.prox_nearest_road_arterial_surface_type IS NOT NULL
            THEN f1.prox_nearest_road_arterial_surface_type
        WHEN
            f1.prox_nearest_road_arterial_surface_type IS NULL
            THEN nn1.prox_nearest_road_arterial_surface_type
        WHEN
            nn1.prox_nearest_road_arterial_surface_type IS NULL
            THEN nn2.prox_nearest_road_arterial_surface_type
    END AS prox_nearest_road_arterial_surface_type,
    CASE
        WHEN
            f1.prox_nearest_road_collector_daily_traffic IS NOT NULL
            THEN f1.prox_nearest_road_collector_daily_traffic
        WHEN
            f1.prox_nearest_road_collector_daily_traffic IS NULL
            THEN nn1.prox_nearest_road_collector_daily_traffic
        WHEN
            nn1.prox_nearest_road_collector_daily_traffic IS NULL
            THEN nn2.prox_nearest_road_collector_daily_traffic
    END AS prox_nearest_road_collector_daily_traffic,
    CASE
        WHEN
            f1.prox_nearest_road_collector_dist_ft IS NOT NULL
            THEN f1.prox_nearest_road_collector_dist_ft
        WHEN
            f1.prox_nearest_road_collector_dist_ft IS NULL
            THEN nn1.prox_nearest_road_collector_dist_ft
        WHEN
            nn1.prox_nearest_road_collector_dist_ft IS NULL
            THEN nn2.prox_nearest_road_collector_dist_ft
    END AS prox_nearest_road_collector_dist_ft,
    CASE
        WHEN
            f1.prox_nearest_road_collector_lanes IS NOT NULL
            THEN f1.prox_nearest_road_collector_lanes
        WHEN
            f1.prox_nearest_road_collector_lanes IS NULL
            THEN nn1.prox_nearest_road_collector_lanes
        WHEN
            nn1.prox_nearest_road_collector_lanes IS NULL
            THEN nn2.prox_nearest_road_collector_lanes
    END AS prox_nearest_road_collector_lanes,
    CASE
        WHEN
            f1.prox_nearest_road_collector_speed_limit IS NOT NULL
            THEN f1.prox_nearest_road_collector_speed_limit
        WHEN
            f1.prox_nearest_road_collector_speed_limit IS NULL
            THEN nn1.prox_nearest_road_collector_speed_limit
        WHEN
            nn1.prox_nearest_road_collector_speed_limit IS NULL
            THEN nn2.prox_nearest_road_collector_speed_limit
    END AS prox_nearest_road_collector_speed_limit,
    CASE
        WHEN
            f1.prox_nearest_road_collector_surface_type IS NOT NULL
            THEN f1.prox_nearest_road_collector_surface_type
        WHEN
            f1.prox_nearest_road_collector_surface_type IS NULL
            THEN nn1.prox_nearest_road_collector_surface_type
        WHEN
            nn1.prox_nearest_road_collector_surface_type IS NULL
            THEN nn2.prox_nearest_road_collector_surface_type
    END AS prox_nearest_road_collector_surface_type,
    CASE
        WHEN
            f1.prox_nearest_road_highway_daily_traffic IS NOT NULL
            THEN f1.prox_nearest_road_highway_daily_traffic
        WHEN
            f1.prox_nearest_road_highway_daily_traffic IS NULL
            THEN nn1.prox_nearest_road_highway_daily_traffic
        WHEN
            nn1.prox_nearest_road_highway_daily_traffic IS NULL
            THEN nn2.prox_nearest_road_highway_daily_traffic
    END AS prox_nearest_road_highway_daily_traffic,
    CASE
        WHEN
            f1.prox_nearest_road_highway_dist_ft IS NOT NULL
            THEN f1.prox_nearest_road_highway_dist_ft
        WHEN
            f1.prox_nearest_road_highway_dist_ft IS NULL
            THEN nn1.prox_nearest_road_highway_dist_ft
        WHEN
            nn1.prox_nearest_road_highway_dist_ft IS NULL
            THEN nn2.prox_nearest_road_highway_dist_ft
    END AS prox_nearest_road_highway_dist_ft,
    CASE
        WHEN
            f1.prox_nearest_road_highway_lanes IS NOT NULL
            THEN f1.prox_nearest_road_highway_lanes
        WHEN
            f1.prox_nearest_road_highway_lanes IS NULL
            THEN nn1.prox_nearest_road_highway_lanes
        WHEN
            nn1.prox_nearest_road_highway_lanes IS NULL
            THEN nn2.prox_nearest_road_highway_lanes
    END AS prox_nearest_road_highway_lanes,
    CASE
        WHEN
            f1.prox_nearest_road_highway_speed_limit IS NOT NULL
            THEN f1.prox_nearest_road_highway_speed_limit
        WHEN
            f1.prox_nearest_road_highway_speed_limit IS NULL
            THEN nn1.prox_nearest_road_highway_speed_limit
        WHEN
            nn1.prox_nearest_road_highway_speed_limit IS NULL
            THEN nn2.prox_nearest_road_highway_speed_limit
    END AS prox_nearest_road_highway_speed_limit,
    CASE
        WHEN
            f1.prox_nearest_road_highway_surface_type IS NOT NULL
            THEN f1.prox_nearest_road_highway_surface_type
        WHEN
            f1.prox_nearest_road_highway_surface_type IS NULL
            THEN nn1.prox_nearest_road_highway_surface_type
        WHEN
            nn1.prox_nearest_road_highway_surface_type IS NULL
            THEN nn2.prox_nearest_road_highway_surface_type
    END AS prox_nearest_road_highway_surface_type,
    CASE
        WHEN
            f1.prox_nearest_secondary_road_dist_ft IS NOT NULL
            THEN f1.prox_nearest_secondary_road_dist_ft
        WHEN
            f1.prox_nearest_secondary_road_dist_ft IS NULL
            THEN nn1.prox_nearest_secondary_road_dist_ft
        WHEN
            nn1.prox_nearest_secondary_road_dist_ft IS NULL
            THEN nn2.prox_nearest_secondary_road_dist_ft
    END AS prox_nearest_secondary_road_dist_ft,
    CASE
        WHEN
            f1.prox_nearest_stadium_dist_ft IS NOT NULL
            THEN f1.prox_nearest_stadium_dist_ft
        WHEN
            f1.prox_nearest_stadium_dist_ft IS NULL
            THEN nn1.prox_nearest_stadium_dist_ft
        WHEN
            nn1.prox_nearest_stadium_dist_ft IS NULL
            THEN nn2.prox_nearest_stadium_dist_ft
    END AS prox_nearest_stadium_dist_ft,
    CASE
        WHEN
            f1.prox_nearest_university_dist_ft IS NOT NULL
            THEN f1.prox_nearest_university_dist_ft
        WHEN
            f1.prox_nearest_university_dist_ft IS NULL
            THEN nn1.prox_nearest_university_dist_ft
        WHEN
            nn1.prox_nearest_university_dist_ft IS NULL
            THEN nn2.prox_nearest_university_dist_ft
    END AS prox_nearest_university_dist_ft,
    CASE
        WHEN
            f1.prox_nearest_vacant_land_dist_ft IS NOT NULL
            THEN f1.prox_nearest_vacant_land_dist_ft
        WHEN
            f1.prox_nearest_vacant_land_dist_ft IS NULL
            THEN nn1.prox_nearest_vacant_land_dist_ft
        WHEN
            nn1.prox_nearest_vacant_land_dist_ft IS NULL
            THEN nn2.prox_nearest_vacant_land_dist_ft
    END AS prox_nearest_vacant_land_dist_ft,
    CASE
        WHEN
            f1.prox_nearest_water_dist_ft IS NOT NULL
            THEN f1.prox_nearest_water_dist_ft
        WHEN
            f1.prox_nearest_water_dist_ft IS NULL
            THEN nn1.prox_nearest_water_dist_ft
        WHEN
            nn1.prox_nearest_water_dist_ft IS NULL
            THEN nn2.prox_nearest_water_dist_ft
    END AS prox_nearest_water_dist_ft,
    f1.shp_parcel_centroid_dist_ft_sd,
    f1.shp_parcel_edge_len_ft_sd,
    f1.shp_parcel_interior_angle_sd,
    f1.shp_parcel_mrr_area_ratio,
    f1.shp_parcel_mrr_side_ratio,
    f1.shp_parcel_num_vertices,
    f1.acs5_count_sex_total,
    f1.acs5_percent_age_children,
    f1.acs5_percent_age_senior,
    f1.acs5_median_age_total,
    f1.acs5_percent_mobility_no_move,
    f1.acs5_percent_mobility_moved_in_county,
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
    f1.other_distressed_community_index,
    f1.other_affordability_risk_index,
    f1.other_tax_bill_amount_total,
    f1.other_tax_bill_rate,
    CASE
        WHEN
            f1.other_school_district_elementary_avg_rating IS NOT NULL
            THEN f1.other_school_district_elementary_avg_rating
        WHEN
            f1.other_school_district_elementary_avg_rating IS NULL
            THEN nn1.other_school_district_elementary_avg_rating
        WHEN
            nn1.other_school_district_elementary_avg_rating IS NULL
            THEN nn2.other_school_district_elementary_avg_rating
    END AS other_school_district_elementary_avg_rating,
    CASE
        WHEN
            f1.other_school_district_secondary_avg_rating IS NOT NULL
            THEN f1.other_school_district_secondary_avg_rating
        WHEN
            f1.other_school_district_secondary_avg_rating IS NULL
            THEN nn1.other_school_district_secondary_avg_rating
        WHEN
            nn1.other_school_district_secondary_avg_rating IS NULL
            THEN nn2.other_school_district_secondary_avg_rating
    END AS other_school_district_secondary_avg_rating,
    -- Exemptions data is usually missing for the 1 or 2 years prior
    -- to the lien date, so we need to fill missing values w/ down up fill
    -- This assumes that people currently receiving exemptions keep them
    COALESCE(
        f1.ccao_is_active_exe_homeowner,
        LAG(f1.ccao_is_active_exe_homeowner)
            IGNORE NULLS
            OVER (PARTITION BY f1.meta_pin ORDER BY f1.meta_year)
    ) AS ccao_is_active_exe_homeowner,
    COALESCE(
        f1.ccao_n_years_exe_homeowner,
        LAG(f1.ccao_n_years_exe_homeowner)
            IGNORE NULLS
            OVER (PARTITION BY f1.meta_pin ORDER BY f1.meta_year)
    ) AS ccao_n_years_exe_homeowner,
    f1.ccao_is_corner_lot,
    f1.meta_year AS year
FROM forward_fill AS f1
LEFT JOIN (
    SELECT *
    FROM forward_fill
    /* Unfortunately, some res parcels are not unique by pin10, card, and
    year. This complicates one of our core assumptions about the res parcel
    universe.
    See https://github.com/ccao-data/data-architecture/issues/558 for more
    information. */
    WHERE SUBSTR(meta_pin, 11, 4) = '0000'
        AND nearest_neighbor_1_dist_ft <= 500
) AS nn1
    ON f1.nearest_neighbor_1_pin10 = nn1.meta_pin10
    AND f1.meta_year = nn1.meta_year
LEFT JOIN (
    SELECT *
    FROM forward_fill
    WHERE SUBSTR(meta_pin, 11, 4) = '0000'
        AND nearest_neighbor_2_dist_ft <= 500
) AS nn2
    ON f1.nearest_neighbor_1_pin10 = nn2.meta_pin10
    AND f1.meta_year = nn2.meta_year
