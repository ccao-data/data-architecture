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
    uni.pin,
    uni.township_name AS meta_township_name,
    LOWER(uni.triad_name) AS meta_triad_name,
    uni.class AS parcel_class,
    pin_cd.class_desc AS parcel_class_description,
    -- Two possible reasons we would decline to build a PINVAL report for a PIN:
    --
    --   1. No representation of the PIN in assessment_card because it is
    --      not a regression class and so was excluded from the assessment set
    --   2. PIN tri is not up for reassessment
    --        - These PINs are still included in the assessment set, they just
    --          do not receive final model values
    --
    -- It's important that we get this right because PINVAL reports will
    -- use this indicator to determine whether to render a report. As such,
    -- the conditions in this column are a bit more lax than the conditions
    -- in the `reason_report_ineligible` column, because we want to catch cases
    -- where PINs are unexpectedly eligible for reports.
    --
    -- Also note that the 'unknown' conditional branch for
    -- the `reason_report_ineligible` column mirrors this logic in its column
    -- definition, so if you change this logic, you should also change that
    -- conditional branch
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
        -- a valid reason for a report to not be available, so we report it
        -- for lack of the true reason why the report is missing
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
            AND LOWER(uni.triad_name) = LOWER(run.assessment_triad)
            THEN NULL
        ELSE 'unknown'
    END AS reason_report_ineligible,
    ac.*,
    ap.pred_pin_final_fmv_round,
    CAST(
        ROUND(
            ac.pred_card_initial_fmv / NULLIF(ac.char_bldg_sf, 0), 0
        ) AS INTEGER
    )
        AS pred_card_initial_fmv_per_sqft,
    ap.loc_property_address AS property_address,
    CONCAT(CAST(ac.char_class AS VARCHAR), ': ', card_cd.class_desc)
        AS char_class_detailed,
    elem_sd.name AS school_elementary_district_name,
    sec_sd.name AS school_secondary_district_name,
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
