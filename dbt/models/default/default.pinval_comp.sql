{{
    config(
        materialized='table'
    )
}}

WITH run_ids_to_include AS (
    SELECT run_id
    FROM {{ source('model', 'metadata') }}
    -- This will eventually grab all run_ids where
    -- run_type == comps
    WHERE run_id = '2025-02-11-charming-eric'
),

raw_comp AS (
    SELECT *
    FROM model.comp
    WHERE run_id IN (SELECT run_id FROM run_ids_to_include)
),

-- Pivot comp_pin
pivoted_comp AS (

    SELECT  pin,
            card,
            1 AS comp_num,
            comp_pin_1           AS comp_pin,
            comp_score_1         AS comp_score,
            comp_document_num_1  AS comp_document_num,
            year,
            run_id
    FROM raw_comp

    UNION ALL
    SELECT  pin, card, 2, comp_pin_2, comp_score_2, comp_document_num_2, year, run_id
    UNION ALL
    SELECT  pin, card, 3, comp_pin_3, comp_score_3, comp_document_num_3, year, run_id
    UNION ALL
    SELECT  pin, card, 4, comp_pin_4, comp_score_4, comp_document_num_4, year, run_id
    UNION ALL
    SELECT  pin, card, 5, comp_pin_5, comp_score_5, comp_document_num_5, year, run_id
),

school_data AS (
    SELECT
        pin10 AS school_pin,
        year,
        school_elementary_district_name,
        school_secondary_district_name
    FROM location.school
    WHERE year > '2014'
),

-- Join with training data
comp_with_training_chars AS (
    SELECT
        pc.run_id,
        pc.pin,
        pc.card,
        pc.comp_num,
        pc.comp_pin,
        pc.comp_document_num,
        pc.year,
        COALESCE(pc.pin = pc.comp_pin, FALSE) AS is_subject_pin_sale,
        CASE
            WHEN train.ind_pin_is_multicard = TRUE THEN 'Subject card'
            ELSE 'Subject property'
        END AS property_label,
        train.loc_property_address AS property_address,
        train.meta_class AS char_class,
        train.meta_nbhd_code,
        train.char_yrblt,
        train.char_bldg_sf,
        train.char_land_sf,
        train.char_beds,
        train.char_fbath,
        train.char_hbath,
        train.loc_longitude,
        train.loc_latitude,
        train.meta_sale_price AS sale_price,
        train.meta_sale_date AS sale_date,
        CAST(CAST(train.meta_sale_price / 1000 AS BIGINT) AS VARCHAR)
        || 'K' AS sale_price_short,
        ROUND(train.meta_sale_price / NULLIF(train.char_bldg_sf, 0))
            AS sale_price_per_sq_ft,
        train.meta_township_code,
        train.meta_sale_count_past_n_years,
        train.char_air,
        train.char_apts,
        train.char_attic_fnsh,
        train.char_attic_type,
        train.char_bsmt,
        train.char_bsmt_fin,
        train.char_ext_wall,
        train.char_frpl,
        train.char_gar1_att,
        train.char_gar1_cnst,
        train.char_gar1_size,
        train.char_heat,
        train.char_ncu,
        train.char_porch,
        train.char_roof_cnst,
        train.char_rooms,
        train.char_tp_dsgn,
        train.char_type_resd,
        train.char_recent_renovation,
        train.loc_census_tract_geoid,
        train.loc_env_flood_fs_factor,
        train.loc_env_airport_noise_dnl,
        train.loc_school_elementary_district_geoid,
        train.loc_school_secondary_district_geoid,
        train.loc_access_cmap_walk_nta_score,
        train.loc_access_cmap_walk_total_score,
        train.loc_tax_municipality_name,
        train.prox_num_pin_in_half_mile,
        train.prox_num_bus_stop_in_half_mile,
        train.prox_num_foreclosure_per_1000_pin_past_5_years,
        train.prox_num_school_in_half_mile,
        train.prox_num_school_with_rating_in_half_mile,
        train.prox_avg_school_rating_in_half_mile,
        train.prox_airport_dnl_total,
        train.prox_nearest_bike_trail_dist_ft,
        train.prox_nearest_cemetery_dist_ft,
        train.prox_nearest_cta_route_dist_ft,
        train.prox_nearest_cta_stop_dist_ft,
        train.prox_nearest_hospital_dist_ft,
        train.prox_lake_michigan_dist_ft,
        train.prox_nearest_major_road_dist_ft,
        train.prox_nearest_metra_route_dist_ft,
        train.prox_nearest_metra_stop_dist_ft,
        train.prox_nearest_park_dist_ft,
        train.prox_nearest_road_arterial_daily_traffic,
        train.prox_nearest_road_collector_daily_traffic,
        train.prox_nearest_new_construction_dist_ft,
        train.prox_nearest_stadium_dist_ft,
        train.prox_nearest_railroad_dist_ft,
        train.prox_nearest_road_highway_dist_ft,
        train.prox_nearest_road_arterial_dist_ft,
        train.prox_nearest_road_collector_dist_ft,
        train.prox_nearest_secondary_road_dist_ft,
        train.prox_nearest_university_dist_ft,
        train.prox_nearest_vacant_land_dist_ft,
        train.prox_nearest_water_dist_ft,
        train.prox_nearest_golf_course_dist_ft,
        train.shp_parcel_centroid_dist_ft_sd,
        train.shp_parcel_edge_len_ft_sd,
        train.shp_parcel_interior_angle_sd,
        train.shp_parcel_mrr_area_ratio,
        train.shp_parcel_mrr_side_ratio,
        train.shp_parcel_num_vertices,
        train.acs5_percent_age_children,
        train.acs5_percent_age_senior,
        train.acs5_median_age_total,
        train.acs5_percent_mobility_no_move,
        train.acs5_percent_mobility_moved_from_other_state,
        train.acs5_percent_household_family_married,
        train.acs5_percent_household_nonfamily_alone,
        train.acs5_percent_education_high_school,
        train.acs5_percent_education_bachelor,
        train.acs5_percent_education_graduate,
        train.acs5_percent_income_below_poverty_level,
        train.acs5_median_income_household_past_year,
        train.acs5_median_income_per_capita_past_year,
        train.acs5_percent_income_household_received_snap_past_year,
        train.acs5_percent_employment_unemployed,
        train.acs5_median_household_total_occupied_year_built,
        train.acs5_median_household_renter_occupied_gross_rent,
        train.acs5_percent_household_owner_occupied,
        train.acs5_percent_household_total_occupied_w_sel_cond,
        train.acs5_percent_mobility_moved_in_county,
        train.other_tax_bill_rate,
        train.other_school_district_elementary_avg_rating,
        train.other_school_district_secondary_avg_rating,
        train.ccao_is_active_exe_homeowner,
        train.ccao_is_corner_lot,
        train.ccao_n_years_exe_homeowner,
        train.time_sale_year,
        train.time_sale_day,
        train.time_sale_quarter_of_year,
        train.time_sale_month_of_year,
        train.time_sale_day_of_year,
        train.time_sale_day_of_month,
        train.time_sale_day_of_week,
        train.time_sale_post_covid,

        school.school_elementary_district_name
            AS loc_school_elementary_district_name,
        school.school_secondary_district_name
            AS loc_school_secondary_district_name
    FROM pivoted_comp AS pc
    LEFT JOIN model.pinval_test_training_data AS train
        ON pc.comp_pin = train.meta_pin
        AND pc.comp_document_num = train.meta_sale_document_num
    LEFT JOIN school_data AS school
        ON SUBSTRING(pc.comp_pin, 1, 10) = school.school_pin
        AND train.meta_year = school.year
)

SELECT * FROM comp_with_training_chars
