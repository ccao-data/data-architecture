-- This view collects assessment data at the card-level, only for final model
-- runs
SELECT
    ac.*,
    town.triad_code AS meta_triad_code,
    fm.type AS model_type
FROM {{ source('model', 'assessment_card') }} AS ac
INNER JOIN {{ ref('model.final_model') }} AS fm
    ON ac.run_id = fm.run_id
    -- Model runs are specific to townships
    AND CONTAINS(fm.township_code_coverage, ac.township_code)
LEFT JOIN {{ source('spatial', 'township') }} AS town
    ON ac.township_code = town.township_code
