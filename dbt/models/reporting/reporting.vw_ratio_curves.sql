-- Final model run IDs by township and year
WITH final_models AS (
    SELECT
        final_model.run_id,
        towns.township_code
    FROM {{ source('model', 'final_model') }} AS final_model
    CROSS JOIN
        UNNEST(final_model.township_code_coverage) AS towns (township_code)
    -- Year will need to be adjusted so that desk review to model values join in
    -- R script is 1 to 1.
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
        assessment_pin.sale_ratio_study_document_num AS sale_document_number
    FROM {{ source('model', 'assessment_pin') }} AS assessment_pin
    INNER JOIN final_models
        ON assessment_pin.run_id = final_models.run_id
        AND assessment_pin.township_code = final_models.township_code
),

-- Res Val provides PINs that sometimes only appear in 2025 or 2026 in
-- default.vw_pin_universe. Make sure we grab one and only one row for every PIN
-- that appears in either year, regardless of whether they have a model value.
most_recent_pin AS (
    SELECT
        uni.pin,
        uni.township_name,
        uni.nbhd_code AS neighborhood_number,
        ROW_NUMBER() OVER (
            PARTITION BY
                uni.pin
            ORDER BY uni.year DESC
        ) AS rank
    FROM {{ ref('default.vw_pin_universe') }} AS uni
    WHERE CAST(uni.year AS INT) IN (YEAR(CURRENT_DATE) - 1, YEAR(CURRENT_DATE))
)

SELECT
    vpu.pin,
    vpu.township_name,
    vpu.neighborhood_number,
    model_vals.model_value,
    model_vals.sale_price,
    model_vals.sale_date,
    model_vals.sale_document_number
FROM most_recent_pin AS vpu
LEFT JOIN model_vals
    ON vpu.pin = model_vals.pin
WHERE vpu.rank = 1
