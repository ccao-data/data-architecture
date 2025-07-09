WITH runs_to_include AS (
    SELECT
        run_id,
        model_predictor_all_name,
        assessment_year,
        assessment_data_year,
        assessment_triad
    FROM {{ source('model', 'metadata') }}
    WHERE run_id = '2025-02-11-charming-eric'
),

final_model_run AS (
    SELECT
        year,
        triad_code,
        SUBSTRING(run_id, 1, 10) AS final_model_run_date
    FROM {{ ref('model.final_model') }}
    WHERE type = 'res'
        AND is_final
),

school_districts AS (
    SELECT
        geoid,
        year,
        MAX(name) AS name
    FROM {{ source('spatial', 'school_district') }}
    WHERE geoid IS NOT NULL
    GROUP BY geoid, year
)

SELECT
    -- For essential attributes like PIN and class, fall back to values from
    -- `default.vw_pin_universe` when no row exists in `model.assesssment_card`
    -- so we can ensure a row for every card regardless of whether it was
    -- included in the assessment set for a given model run. We need these
    -- essential attrs even when parcels aren't in the assessment set in order
    -- to generate detailed descriptions for why those parcels don't have
    -- reports
    COALESCE(ac.meta_pin, uni.pin) AS meta_pin,
    COALESCE(ac.township_code, uni.township_code) AS meta_township_code,
    uni.township_name AS meta_township_name,
    LOWER(uni.triad_name) AS meta_triad_name,
    COALESCE(ac.char_class, uni.class) AS char_class,
    COALESCE(card_cd.class_desc, pin_cd.class_desc) AS char_class_desc,
    -- Three possible reasons we would decline to build a PINVAL report for a
    -- PIN:
    --
    --   1. No representation of the PIN in assessment_card because it is
    --      not a regression class and so was excluded from the assessment set
    --   2. PIN has a row in `model.assessment_card`, but no card number,
    --      indicating a rare data error
    --   3. PIN tri is not up for reassessment
    --        - These PINs are still included in the assessment set, they just
    --          do not receive final model values
    --
    -- It's important that we get this right because PINVAL reports will
    -- use this indicator to determine whether to render a report. As such,
    -- the conditions in this column are a bit more lax than the conditions
    -- in the `reason_report_ineligible` column, because we want to catch cases
    -- where PINs are unexpectedly eligible for reports.
    --
    -- Also note that the 'unknown' conditional case for
    -- the `reason_report_ineligible` column mirrors this logic in its column
    -- definition, so if you change this logic, you should also change that
    -- conditional case
    (
        ac.meta_pin IS NOT NULL
        AND ac.meta_card_num IS NOT NULL
        AND LOWER(uni.triad_name) = LOWER(run.assessment_triad)
    ) AS is_report_eligible,
    CASE
        -- In some rare cases the parcel class can be different from
        -- the card class, in which case these class explanations are not
        -- guaranteed to be the true reason that a report is missing. But
        -- in those cases, a non-regression class for the PIN should still be
        -- a valid reason for a report to be unavailable, so we report it
        -- as a best guess at true reason why the report is missing
        WHEN uni.class IN ('299') THEN 'condo'
        WHEN
            pin_cd.class_code IS NULL  -- Class is not in our class dict
            OR NOT pin_cd.regression_class
            OR (pin_cd.modeling_group NOT IN ('SF', 'MF'))
            THEN 'non_regression_class'
        WHEN LOWER(uni.triad_name) != LOWER(run.assessment_triad) THEN 'non_tri'
        WHEN ac.meta_card_num IS NULL THEN 'missing_card'
        WHEN
            ac.meta_pin IS NOT NULL
            AND ac.meta_card_num IS NOT NULL
            AND LOWER(uni.triad_name) = LOWER(run.assessment_triad)
            THEN NULL
        ELSE 'unknown'
    END AS reason_report_ineligible,
    -- Select all predictors from `model.assessment_card`. Unfortunately we
    -- have to add predictors to this list manually whenever we add them to
    -- the model, but we have a data integrity test on this table that should
    -- alert us if we ever fall out of sync with the model.
    --
    -- Never remove predictors from this list, only add them. Outdated
    -- predictors are most likely necessary to support reports for prior
    -- assessment years
    ac.meta_nbhd_code,
    ac.meta_sale_count_past_n_years,
    ac.char_yrblt,
    ac.char_air,
    ac.char_apts,
    ac.char_attic_fnsh,
    ac.char_attic_type,
    ac.char_beds,
    ac.char_bldg_sf,
    ac.char_bsmt,
    ac.char_bsmt_fin,
    ac.char_ext_wall,
    ac.char_fbath,
    ac.char_frpl,
    ac.char_gar1_att,
    ac.char_gar1_cnst,
    ac.char_gar1_size,
    ac.char_hbath,
    ac.char_land_sf,
    ac.char_heat,
    ac.char_ncu,
    ac.char_porch,
    ac.char_roof_cnst,
    ac.char_rooms,
    ac.char_tp_dsgn,
    ac.char_type_resd,
    ac.char_recent_renovation,
    ac.loc_longitude,
    ac.loc_latitude,
    ac.loc_census_tract_geoid,
    ac.loc_env_flood_fs_factor,
    ac.loc_school_elementary_district_geoid,
    ac.loc_school_secondary_district_geoid,
    ac.loc_access_cmap_walk_nta_score,
    ac.loc_access_cmap_walk_total_score,
    ac.loc_tax_municipality_name,
    ac.prox_num_pin_in_half_mile,
    ac.prox_num_bus_stop_in_half_mile,
    ac.prox_num_foreclosure_per_1000_pin_past_5_years,
    ac.prox_avg_school_rating_in_half_mile,
    ac.prox_airport_dnl_total,
    ac.prox_nearest_bike_trail_dist_ft,
    ac.prox_nearest_cemetery_dist_ft,
    ac.prox_nearest_cta_route_dist_ft,
    ac.prox_nearest_cta_stop_dist_ft,
    ac.prox_nearest_hospital_dist_ft,
    ac.prox_lake_michigan_dist_ft,
    ac.prox_nearest_metra_route_dist_ft,
    ac.prox_nearest_metra_stop_dist_ft,
    ac.prox_nearest_park_dist_ft,
    ac.prox_nearest_railroad_dist_ft,
    ac.prox_nearest_university_dist_ft,
    ac.prox_nearest_vacant_land_dist_ft,
    ac.prox_nearest_water_dist_ft,
    ac.prox_nearest_golf_course_dist_ft,
    ac.prox_nearest_road_highway_dist_ft,
    ac.prox_nearest_road_arterial_dist_ft,
    ac.prox_nearest_road_collector_dist_ft,
    ac.prox_nearest_road_arterial_daily_traffic,
    ac.prox_nearest_road_collector_daily_traffic,
    ac.prox_nearest_new_construction_dist_ft,
    ac.prox_nearest_stadium_dist_ft,
    ac.acs5_percent_age_children,
    ac.acs5_percent_age_senior,
    ac.acs5_median_age_total,
    ac.acs5_percent_household_family_married,
    ac.acs5_percent_household_nonfamily_alone,
    ac.acs5_percent_education_high_school,
    ac.acs5_percent_education_bachelor,
    ac.acs5_percent_education_graduate,
    ac.acs5_percent_income_below_poverty_level,
    ac.acs5_median_income_household_past_year,
    ac.acs5_median_income_per_capita_past_year,
    ac.acs5_percent_income_household_received_snap_past_year,
    ac.acs5_percent_employment_unemployed,
    ac.acs5_median_household_total_occupied_year_built,
    ac.acs5_median_household_renter_occupied_gross_rent,
    ac.acs5_percent_household_owner_occupied,
    ac.other_tax_bill_rate,
    ac.time_sale_year,
    ac.time_sale_day,
    ac.time_sale_quarter_of_year,
    ac.time_sale_month_of_year,
    ac.time_sale_day_of_year,
    ac.time_sale_day_of_month,
    ac.time_sale_day_of_week,
    ac.time_sale_post_covid,
    ac.shp_parcel_centroid_dist_ft_sd,
    ac.shp_parcel_edge_len_ft_sd,
    ac.shp_parcel_interior_angle_sd,
    ac.shp_parcel_mrr_area_ratio,
    ac.shp_parcel_mrr_side_ratio,
    ac.shp_parcel_num_vertices,
    ap.pred_pin_final_fmv_round,
    -- Pull some additional parcel-level info from `model.assessment_pin`
    CAST(
        ROUND(
            ac.pred_card_initial_fmv / NULLIF(ac.char_bldg_sf, 0), 0
        ) AS INTEGER
    )
        AS pred_card_initial_fmv_per_sqft,
    ap.loc_property_address AS property_address,
    CAST(ap.meta_pin_num_cards AS INTEGER) AS ap_meta_pin_num_cards,
    -- Format some card-level predictors to make them more interpretable to
    -- non-technical users
    CONCAT(CAST(ac.char_class AS VARCHAR), ': ', card_cd.class_desc)
        AS char_class_detailed,
    COALESCE(
        CAST(run.assessment_year AS INTEGER) >= 2025
        AND ap.meta_pin_num_cards IN (2, 3), FALSE
    ) AS is_parcel_small_multicard,
    COALESCE(
        CAST(run.assessment_year AS INTEGER) >= 2025
        AND ap.meta_pin_num_cards IN (2, 3)
        AND ROW_NUMBER() OVER (
            PARTITION BY ac.meta_pin, ac.run_id
            ORDER BY COALESCE(ac.char_bldg_sf, 0) DESC, ac.meta_card_num ASC
        ) = 1, FALSE
    ) AS is_frankencard,
    CASE
        WHEN CAST(run.assessment_year AS INTEGER) >= 2025
            AND ap.meta_pin_num_cards IN (2, 3)
            THEN SUM(COALESCE(ac.char_bldg_sf, 0)) OVER (
                PARTITION BY ac.meta_pin, ac.run_id
            )
    END AS combined_bldg_sf,
    elem_sd.name AS school_elementary_district_name,
    sec_sd.name AS school_secondary_district_name,
    -- Pull model run metadata from `model.metadata` and `model.final_model`.
    -- This metadata will be duplicated across all cards in a model run, but
    -- that's fine because this table is only ever intended to be used to
    -- extract individual rows and use those rows as the basis for PINVAL
    -- reports, in which case row-level duplication is useful
    run.run_id AS model_run_id,
    run.model_predictor_all_name,
    run.assessment_triad AS assessment_triad_name,
    run.assessment_year,
    final.final_model_run_date
-- We use pin_universe as the base for the query rather than assessment_card
-- because we need to generate explanations for why reports are missing if a
-- PIN is valid but not in assessment_card
FROM {{ ref('default.vw_pin_universe') }} AS uni
INNER JOIN runs_to_include AS run
-- We use prior year characteristics for model predictors, so we need to
-- pull parcel information based on the model's data year, not its
-- assessment year
    ON uni.year = run.assessment_data_year
LEFT JOIN {{ source('model', 'assessment_card') }} AS ac
    ON run.run_id = ac.run_id
    AND uni.pin = ac.meta_pin
    AND uni.year = ac.meta_year
LEFT JOIN {{ source('model', 'assessment_pin') }} AS ap
    ON ac.meta_pin = ap.meta_pin
    AND ac.run_id = ap.run_id
LEFT JOIN school_districts AS elem_sd
    ON ac.loc_school_elementary_district_geoid = elem_sd.geoid
    AND ac.meta_year = elem_sd.year
LEFT JOIN school_districts AS sec_sd
    ON ac.loc_school_secondary_district_geoid = sec_sd.geoid
    AND ac.meta_year = sec_sd.year
LEFT JOIN final_model_run AS final
    ON run.assessment_year = final.year
-- Join to class dict twice, since PIN class and card class can be different
LEFT JOIN {{ ref('ccao.class_dict') }} AS pin_cd
    ON uni.class = pin_cd.class_code
LEFT JOIN {{ ref('ccao.class_dict') }} AS card_cd
    ON ac.char_class = card_cd.class_code
