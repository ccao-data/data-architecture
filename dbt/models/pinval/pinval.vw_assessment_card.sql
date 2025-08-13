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
    -- Join to `runs_to_include` so that we can assign assessment triad and
    -- year information to all PINs, even if they're missing from the
    -- assessment set.
    --
    -- We use an inner join so that we only return PINs for years that are
    -- report-enabled
    INNER JOIN (
        SELECT
            *,
            ROW_NUMBER()
                -- When an assessment year has two models, pick the more recent
                -- one to use for extracting metadata. This works because the
                -- metadata that this subquery extracts should be identical for
                -- all final models in a year
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

-- Count cards for each PIN. We need to do this in a subquery because newer
-- model runs save this value to `model.assessment_pin`, but we need to compute
-- it for older runs that did not save the value
card_count AS (
    SELECT
        ap.meta_pin,
        ap.run_id,
        CAST(
            COALESCE(
                ap.meta_pin_num_cards,
                card_count.meta_pin_num_cards
            )
            AS INTEGER
        ) AS meta_pin_num_cards
    FROM {{ source('model', 'assessment_pin') }} AS ap
    LEFT JOIN (
        SELECT
            meta_pin,
            run_id,
            COUNT(*) AS meta_pin_num_cards
        FROM {{ source('model', 'assessment_card') }}
        GROUP BY run_id, meta_pin
    ) AS card_count
        ON ap.meta_pin = card_count.meta_pin
        AND ap.run_id = card_count.run_id
),

-- Determine whether the card is part of a small multicard PIN that was valued
-- after we changed our multicard valuation strategy, which will help us
-- explain its value
card_pin_meta AS (
    SELECT
        ac.meta_pin,
        ac.meta_card_num,
        ac.run_id,
        cc.meta_pin_num_cards,
        COALESCE(
            CAST(ac.assessment_year AS INTEGER) >= 2025
            AND cc.meta_pin_num_cards IN (2, 3), FALSE
        ) AS is_parcel_small_multicard
    FROM assessment_card AS ac
    LEFT JOIN card_count AS cc
        ON ac.meta_pin = cc.meta_pin
        AND ac.run_id = cc.run_id
),

-- Further aggregation for small multicards to use for explaining their
-- valuation
card_agg AS (
    SELECT
        ac.meta_pin,
        ac.meta_card_num,
        ac.run_id,
        cpm.meta_pin_num_cards,
        cpm.is_parcel_small_multicard,
        COALESCE(
            cpm.is_parcel_small_multicard
            AND ROW_NUMBER() OVER (
                PARTITION BY ac.meta_pin, ac.run_id
                ORDER BY COALESCE(ac.char_bldg_sf, 0) DESC, ac.meta_card_num ASC
            ) = 1, FALSE
        ) AS is_frankencard,
        SUM(COALESCE(ac.char_bldg_sf, 0))
            OVER (PARTITION BY ac.meta_pin, ac.run_id)
            AS combined_bldg_sf
    FROM assessment_card AS ac
    LEFT JOIN card_pin_meta AS cpm
        ON ac.meta_pin = cpm.meta_pin
        AND ac.meta_card_num = cpm.meta_card_num
        AND ac.run_id = cpm.run_id
),

-- Get run IDs for SHAPs that we want to include for the purpose of ranking
-- features by importance
shap_runs_to_include AS (
    SELECT
        meta.run_id,
        meta.assessment_year,
        COALESCE(
            final.township_code_coverage,
            -- `township_code_coverage` will only be present if the model is a
            -- final model, but it's possible for SHAP runs to be separate from
            -- the final model run. Handle this using a special indicator 'all'
            -- to mark SHAP runs that are not final models
            ARRAY['all']
        ) AS township_code_coverage
    FROM {{ source('model', 'metadata') }} AS meta
    LEFT JOIN {{ ref('model.final_model') }} AS final
        ON meta.run_id = final.run_id
    WHERE meta.run_id IN (
            '2024-02-06-relaxed-tristan',
            '2024-03-17-stupefied-maya',
            '2025-04-25-fancy-free-billy'
        )
),

-- Query SHAP values for on the runs we want to include
shap AS (
    SELECT
        shap.*,
        run.assessment_year
    FROM {{ source('model', 'shap') }} AS shap
    INNER JOIN (
        SELECT
            run.*,
            t.township_code
        FROM shap_runs_to_include AS run
        -- Handle the use of different model runs for different towns
        CROSS JOIN UNNEST(run.township_code_coverage) AS t (township_code)
    ) AS run
        ON shap.run_id = run.run_id
        AND (
            shap.township_code = run.township_code
            -- Handle non-final SHAP models
            OR run.township_code = 'all'
        )
),

-- Get crosswalk between school district geo IDs and names, so that we can
-- translate the geo IDs in user-facing reports
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
    COALESCE(twn.township_name, uni.township_name) AS meta_township_name,
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
    {{ all_predictors('ac', exclude=['meta_township_code', 'char_class']) }},
    {{ all_predictors('shap', alias_prefix='shap') }},
    CONCAT(CAST(ac.char_class AS VARCHAR), ': ', card_cd.class_desc)
        AS char_class_detailed,
    ap.loc_property_address AS property_address,
    ap.loc_property_city,
    ac.pred_card_initial_fmv,
    CASE
        WHEN card_agg.is_parcel_small_multicard
            THEN CAST(
                ROUND(
                    ac.pred_card_initial_fmv
                    / NULLIF(card_agg.combined_bldg_sf, 0),
                    0
                ) AS INTEGER
            )
        ELSE CAST(
                ROUND(
                    ac.pred_card_initial_fmv / NULLIF(ac.char_bldg_sf, 0),
                    0
                ) AS INTEGER
            )
    END AS pred_card_initial_fmv_per_sqft,
    ap.pred_pin_final_fmv_round,
    CAST(
        ROUND(
            ap.pred_pin_final_fmv_round
            / NULLIF(card_agg.combined_bldg_sf, 0),
            0
        ) AS INTEGER
    ) AS pred_pin_final_fmv_round_per_sqft,
    card_agg.meta_pin_num_cards,
    card_agg.is_parcel_small_multicard,
    card_agg.is_frankencard,
    card_agg.combined_bldg_sf,
    elem_sd.name AS school_elementary_district_name,
    sec_sd.name AS school_secondary_district_name
FROM pin_universe AS uni
FULL OUTER JOIN assessment_card AS ac
    ON uni.pin = ac.meta_pin
    AND uni.year = ac.meta_year
LEFT JOIN {{ source('model', 'assessment_pin') }} AS ap
    ON ac.meta_pin = ap.meta_pin
    AND ac.run_id = ap.run_id
LEFT JOIN card_agg
    ON ac.meta_pin = card_agg.meta_pin
    AND ac.meta_card_num = card_agg.meta_card_num
    AND ac.run_id = card_agg.run_id
LEFT JOIN shap
    ON ac.meta_pin = shap.meta_pin
    AND ac.meta_card_num = shap.meta_card_num
    -- SHAP run IDs can differ from final model run IDs, so use assessment year
    -- to join them
    AND ac.assessment_year = shap.assessment_year
LEFT JOIN school_districts AS elem_sd
    ON ac.loc_school_elementary_district_geoid = elem_sd.geoid
    AND ac.meta_year = elem_sd.year
LEFT JOIN school_districts AS sec_sd
    ON ac.loc_school_secondary_district_geoid = sec_sd.geoid
    AND ac.meta_year = sec_sd.year
LEFT JOIN {{ source('spatial', 'township') }} AS twn
    ON ac.township_code = twn.township_code
-- Join to class dict twice, since PIN class and card class can be different
LEFT JOIN {{ ref('ccao.class_dict') }} AS pin_cd
    ON uni.class = pin_cd.class_code
LEFT JOIN {{ ref('ccao.class_dict') }} AS card_cd
    ON ac.char_class = card_cd.class_code
