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
    FROM "model"."comp"
    WHERE run_id IN (SELECT run_id FROM run_ids_to_include)
    -- subset to test for now
        ---AND pin IN ('08102100010000', '06272070290000', '12364100170000')
),

-- Pivot comp_pin
unpivoted_comp_pin AS (
    SELECT
        pin,
        card,
        1 AS comp_num,
        comp_pin_1 AS comp_pin,
        year,
        run_id
    FROM raw_comp
    UNION ALL
    SELECT
        pin,
        card,
        2 AS comp_num,
        comp_pin_2,
        year,
        run_id
    FROM raw_comp
    UNION ALL
    SELECT
        pin,
        card,
        3 AS comp_num,
        comp_pin_3,
        year,
        run_id
    FROM raw_comp
    UNION ALL
    SELECT
        pin,
        card,
        4 AS comp_num,
        comp_pin_4,
        year,
        run_id
    FROM raw_comp
    UNION ALL
    SELECT
        pin,
        card,
        5 AS comp_num,
        comp_pin_5,
        year,
        run_id
    FROM raw_comp
),

-- Pivot comp_score
unpivoted_comp_score AS (
    SELECT
        pin,
        1 AS comp_num,
        comp_score_1 AS comp_score
    FROM raw_comp
    UNION ALL
    SELECT
        pin,
        2 AS comp_num,
        comp_score_2
    FROM raw_comp
    UNION ALL
    SELECT
        pin,
        3 AS comp_num,
        comp_score_3
    FROM raw_comp
    UNION ALL
    SELECT
        pin,
        4 AS comp_num,
        comp_score_4
    FROM raw_comp
    UNION ALL
    SELECT
        pin,
        5 AS comp_num,
        comp_score_5
    FROM raw_comp
),

-- Pivot comp_document_num
unpivoted_comp_document_num AS (
    SELECT
        pin,
        1 AS comp_num,
        comp_document_num_1 AS comp_document_num
    FROM raw_comp
    UNION ALL
    SELECT
        pin,
        2 AS comp_num,
        comp_document_num_2
    FROM raw_comp
    UNION ALL
    SELECT
        pin,
        3 AS comp_num,
        comp_document_num_3
    FROM raw_comp
    UNION ALL
    SELECT
        pin,
        4 AS comp_num,
        comp_document_num_4
    FROM raw_comp
    UNION ALL
    SELECT
        pin,
        5 AS comp_num,
        comp_document_num_5
    FROM raw_comp
),

-- Combine all pivoted data
pivoted_comp AS (
    SELECT
        p.pin,
        p.card,
        p.comp_num,
        p.comp_pin,
        p.year,
        p.run_id,
        s.comp_score,
        d.comp_document_num
    FROM unpivoted_comp_pin AS p
    INNER JOIN unpivoted_comp_score AS s
        ON p.pin = s.pin AND p.comp_num = s.comp_num
    INNER JOIN unpivoted_comp_document_num AS d
        ON p.pin = d.pin AND p.comp_num = d.comp_num
),

school_data AS (
    SELECT
        pin10 AS school_pin,
        year,
        school_elementary_district_name,
        school_secondary_district_name
    FROM "location"."school"
    WHERE year > '2014'
),

-- Join with training data
comp_with_training_chars AS (
    SELECT
        pc.run_id,
        pc.pin,
        pc.card,
        pc.comp_pin,
        pc.comp_document_num,
        pc.year,
        COALESCE(pc.pin = pc.comp_pin, FALSE) AS is_subject_pin_sale,
        CASE
            WHEN t.ind_pin_is_multicard = TRUE THEN 'Subject card'
            ELSE 'Subject property'
        END AS property_label,
        t.loc_property_address AS property_address,
        t.meta_class AS char_class,
        t.meta_nbhd_code,
        t.char_yrblt,
        t.char_bldg_sf AS bldg_sf,
        t.char_land_sf AS land_sf,
        t.char_beds,
        t.char_fbath,
        t.char_hbath,
        t.loc_longitude,
        t.loc_latitude,
        t.meta_sale_price AS sale_price,
        t.meta_sale_date AS sale_date,
        CAST(ROUND(t.meta_sale_price / 1000) AS VARCHAR)
        || 'K' AS sale_price_short,
        ROUND(t.meta_sale_price / NULLIF(t.char_bldg_sf, 0))
            AS sale_price_per_sq_ft,

        -- All other columns from t.*
        t.meta_township_code,
        t.meta_sale_count_past_n_years,
        t.char_air,
        t.char_apts,
        t.char_attic_fnsh,
        t.char_attic_type,
        t.char_bsmt,
        t.char_bsmt_fin,
        t.char_ext_wall,
        t.char_frpl,
        t.char_gar1_att,
        t.char_gar1_cnst,
        t.char_gar1_size,
        t.char_heat,
        t.char_ncu,
        t.char_porch,
        t.char_roof_cnst,
        t.char_rooms,
        t.char_tp_dsgn,
        t.char_type_resd,
        t.char_recent_renovation,
        t.loc_census_tract_geoid,
        t.loc_env_flood_fs_factor,
        t.loc_env_airport_noise_dnl,
        t.loc_school_elementary_district_geoid,
        t.loc_school_secondary_district_geoid,
        t.loc_access_cmap_walk_nta_score,
        t.loc_access_cmap_walk_total_score,
        t.loc_tax_municipality_name,
        t.prox_num_pin_in_half_mile,
        t.prox_num_bus_stop_in_half_mile,
        t.prox_num_foreclosure_per_1000_pin_past_5_years,
        t.prox_num_school_in_half_mile,
        t.prox_num_school_with_rating_in_half_mile,
        t.prox_avg_school_rating_in_half_mile,
        t.prox_airport_dnl_total,
        t.prox_nearest_bike_trail_dist_ft,
        t.prox_nearest_cemetery_dist_ft,
        t.prox_nearest_cta_route_dist_ft,
        t.prox_nearest_cta_stop_dist_ft,
        t.prox_nearest_hospital_dist_ft,
        t.prox_lake_michigan_dist_ft,
        t.prox_nearest_major_road_dist_ft,
        t.prox_nearest_metra_route_dist_ft,
        t.prox_nearest_metra_stop_dist_ft,
        t.prox_nearest_park_dist_ft,
        t.prox_nearest_railroad_dist_ft,
        t.prox_nearest_secondary_road_dist_ft,
        t.prox_nearest_university_dist_ft,
        t.prox_nearest_vacant_land_dist_ft,
        t.prox_nearest_water_dist_ft,
        t.prox_nearest_golf_course_dist_ft,
        t.acs5_percent_age_children,
        t.acs5_percent_age_senior,
        t.acs5_median_age_total,
        t.acs5_percent_mobility_no_move,
        t.acs5_percent_mobility_moved_from_other_state,
        t.acs5_percent_household_family_married,
        t.acs5_percent_household_nonfamily_alone,
        t.acs5_percent_education_high_school,
        t.acs5_percent_education_bachelor,
        t.acs5_percent_education_graduate,
        t.acs5_percent_income_below_poverty_level,
        t.acs5_median_income_household_past_year,
        t.acs5_median_income_per_capita_past_year,
        t.acs5_percent_income_household_received_snap_past_year,
        t.acs5_percent_employment_unemployed,
        t.acs5_median_household_total_occupied_year_built,
        t.acs5_median_household_renter_occupied_gross_rent,
        t.acs5_percent_household_owner_occupied,
        t.acs5_percent_household_total_occupied_w_sel_cond,
        t.acs5_percent_mobility_moved_in_county,
        t.other_tax_bill_rate,
        t.other_school_district_elementary_avg_rating,
        t.other_school_district_secondary_avg_rating,
        t.ccao_is_active_exe_homeowner,
        t.ccao_is_corner_lot,
        t.ccao_n_years_exe_homeowner,
        t.time_sale_year,
        t.time_sale_day,
        t.time_sale_quarter_of_year,
        t.time_sale_month_of_year,
        t.time_sale_day_of_year,
        t.time_sale_day_of_month,
        t.time_sale_day_of_week,
        t.time_sale_post_covid,

        school.school_elementary_district_name
            AS loc_school_elementary_district_name,
        school.school_secondary_district_name
            AS loc_school_secondary_district_name
    FROM pivoted_comp AS pc
    LEFT JOIN "model"."pinval_test_training_data" AS t
        ON pc.comp_pin = t.meta_pin
        AND pc.comp_document_num = t.meta_sale_document_num
    LEFT JOIN school_data AS school
        ON SUBSTRING(pc.comp_pin, 1, 10) = school.school_pin
        AND t.meta_year = school.year
)

--select distinct school_pin from school_data where year = '2024';
SELECT * FROM comp_with_training_chars
