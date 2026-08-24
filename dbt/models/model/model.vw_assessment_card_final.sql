-- This view collects assessment data for each card, only for final model runs
SELECT
    ac.*,
    fm.type
FROM {{ source('model', 'assessment_card') }} AS ac
INNER JOIN {{ ref('model.final_model') }} AS fm
    ON ac.run_id = fm.run_id
    -- Model runs are specific to townships
    AND CONTAINS(fm.township_code_coverage, ac.township_code)
