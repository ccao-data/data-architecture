-- Get some metadata for the model runs that we want to use as the basis for
-- PINVAL reports
WITH runs_to_include AS (
    SELECT
        meta.run_id,
        meta.model_predictor_all_name,
        meta.assessment_year,
        meta.assessment_data_year,
        meta.assessment_triad,
        SUBSTRING(final.run_id, 1, 10) AS final_model_run_date,
        final.township_code_coverage
    FROM {{ source('model', 'metadata') }} AS meta
    INNER JOIN {{ ref('model.final_model') }} AS final
        ON meta.run_id = final.run_id
    WHERE meta.run_id IN (
            '2024-02-06-relaxed-tristan',
            '2024-03-17-stupefied-maya',
            '2025-02-11-charming-eric'
        )
),

-- Get the universe of PINs we want to produce reports for, even if those PINs
-- are not eligible for reports (in which case we will generate error pages for
-- them explaining why).
pin_universe AS (
    SELECT
        uni.*,
        run.run_id,
        -- These are the only run metadata fields that are necessary to generate
        -- error pages
        run.assessment_year,
        run.assessment_triad
    FROM {{ ref('default.vw_pin_universe') }} AS uni
    INNER JOIN (
        SELECT
            *,
            ROW_NUMBER()
                -- When an assessment year has two models, pick the more recent
                -- one
                OVER (PARTITION BY assessment_data_year ORDER BY run_id DESC)
                AS row_num
        FROM runs_to_include
    ) AS run
    -- We use prior year characteristics for model predictors, so we need to
    -- pull parcel information based on the model's data year, not its
        -- assessment year
        ON uni.year = run.assessment_data_year
        AND run.row_num = 1
),

-- Get the assessment set for each model run that we want to use for reports
assessment_card AS (
    SELECT
        ac.*,
        run.model_predictor_all_name,
        run.assessment_year,
        run.assessment_data_year,
        run.assessment_triad,
        run.final_model_run_date
    FROM {{ source('model', 'assessment_card') }} AS ac
    INNER JOIN (
        SELECT
            run.*,
            t.township_code
        FROM runs_to_include AS run
        -- Handle the use of different model runs for different towns
        CROSS JOIN UNNEST(run.township_code_coverage) AS t (township_code)
    ) AS run
        ON ac.run_id = run.run_id
        AND ac.meta_township_code = run.township_code
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
    ac.meta_card_num,
    COALESCE(ac.township_code, uni.township_code) AS meta_township_code,
    uni.township_name AS meta_township_name,
    LOWER(uni.triad_name) AS meta_triad_name,
    COALESCE(ac.char_class, uni.class) AS char_class,
    COALESCE(card_cd.class_desc, pin_cd.class_desc) AS char_class_desc,
    COALESCE(ac.assessment_year, uni.assessment_year) AS assessment_year,
    COALESCE(ac.assessment_triad, uni.assessment_triad)
        AS assessment_triad_name,
    ac.run_id,
    ac.model_predictor_all_name,
    ac.final_model_run_date,
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
        AND LOWER(uni.triad_name) = LOWER(uni.assessment_triad)
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
        WHEN LOWER(uni.triad_name) != LOWER(uni.assessment_triad) THEN 'non_tri'
        WHEN ac.meta_card_num IS NULL THEN 'missing_card'
        WHEN
            ac.meta_pin IS NOT NULL
            AND ac.meta_card_num IS NOT NULL
            AND LOWER(uni.triad_name) = LOWER(uni.assessment_triad)
            THEN NULL
        ELSE 'unknown'
    END AS reason_report_ineligible,
    {{ all_predictors('ac') }},
    ac.pred_card_initial_fmv,
    ap.pred_pin_final_fmv_round,
    -- Pull some additional parcel-level info from `model.assessment_pin`
    CAST(
        ROUND(
            ac.pred_card_initial_fmv / NULLIF(ac.char_bldg_sf, 0), 0
        ) AS INTEGER
    )
        AS pred_card_initial_fmv_per_sqft,
    ap.loc_property_address AS property_address,
    ap.loc_property_city,
    CAST(ap.meta_pin_num_cards AS INTEGER) AS ap_meta_pin_num_cards,
    -- Format some card-level predictors to make them more interpretable to
    -- non-technical users
    CONCAT(CAST(ac.char_class AS VARCHAR), ': ', card_cd.class_desc)
        AS char_class_detailed,
    COALESCE(
        CAST(ac.assessment_year AS INTEGER) >= 2025
        AND ap.meta_pin_num_cards IN (2, 3), FALSE
    ) AS is_parcel_small_multicard,
    CASE
        WHEN CAST(ac.assessment_year AS INTEGER) >= 2025
            AND ap.meta_pin_num_cards IN (2, 3)
            THEN SUM(COALESCE(ac.char_bldg_sf, 0)) OVER (
                PARTITION BY ac.meta_pin, ac.run_id
            )
    END AS combined_bldg_sf,
    elem_sd.name AS school_elementary_district_name,
    sec_sd.name AS school_secondary_district_name
FROM pin_universe AS uni
FULL OUTER JOIN assessment_card AS ac
    ON uni.pin = ac.meta_pin
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
--- Join to class dict twice, since PIN class and card class can be different
LEFT JOIN {{ ref('ccao.class_dict') }} AS pin_cd
    ON uni.class = pin_cd.class_code
LEFT JOIN {{ ref('ccao.class_dict') }} AS card_cd
    ON ac.char_class = card_cd.class_code
