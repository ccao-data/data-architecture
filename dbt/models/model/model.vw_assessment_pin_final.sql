-- This view collects assessment data at the PIN-level, only for final model
-- runs
SELECT
    ap.*,
    fm.type AS model_type
FROM {{ source('model', 'assessment_pin') }} AS ap
INNER JOIN {{ ref('model.final_model') }} AS fm
    ON ap.run_id = fm.run_id
    -- Model runs are specific to townships
    AND CONTAINS(fm.township_code_coverage, ap.township_code)
