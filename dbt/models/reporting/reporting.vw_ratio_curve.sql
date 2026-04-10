-- Final model run IDs by township and year
WITH final_models AS (
    SELECT
        final_model.run_id,
        towns.township_code
    FROM {{ ref('model.final_model') }} AS final_model
    CROSS JOIN
        UNNEST(final_model.township_code_coverage) AS towns (township_code)
    WHERE CAST(final_model.year AS INT) = YEAR(CURRENT_DATE)
        AND final_model.type = 'res'
),

-- Use final model run IDs to grab the model values we need to compare Res Val's
-- desk review values against
model_vals AS (
    SELECT
        assessment_pin.meta_pin AS pin,
        assessment_pin.pred_pin_final_fmv_round AS model_value,
        assessment_pin.sale_ratio_study_price AS sale_price,
        assessment_pin.sale_ratio_study_date AS sale_date,
        assessment_pin.sale_ratio_study_document_num AS sale_document_number,
        assessment_pin.year
    FROM {{ source('model', 'assessment_pin') }} AS assessment_pin
    INNER JOIN final_models
        ON assessment_pin.run_id = final_models.run_id
        AND assessment_pin.township_code = final_models.township_code
)

SELECT
    vpu.pin,
    vpu.township_name,
    vpu.nbhd_code AS neighborhood_number,
    -- These values are slightly different than the desk review values Res Val
    -- provides due to rounding in iasWorld, but the differences are negligible
    CAST(vpv.pre_mailed_tot * 10 AS BIGINT) AS desk_review_value,
    CAST(model_vals.model_value AS BIGINT) AS model_value,
    CAST(model_vals.sale_price AS BIGINT) AS sale_price,
    model_vals.sale_date,
    model_vals.sale_document_number
FROM {{ ref('default.vw_pin_universe') }} AS vpu
-- Inner joins to only pull PINs that have both model and desk review values
-- This should also ensure that we are only pulling regression class parcels
INNER JOIN model_vals
    ON vpu.pin = model_vals.pin
    AND vpu.year = model_vals.year
INNER JOIN {{ ref('default.vw_pin_value') }} AS vpv
    ON vpu.pin = vpv.pin
    AND vpu.year = vpv.year
    AND vpv.pre_mailed_tot IS NOT NULL
