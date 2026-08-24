-- This view collects training data for each PIN, only for final model runs
SELECT
    td.*,
    fm.type
FROM {{ ref('model.training_data') }} AS td
INNER JOIN {{ ref('model.final_model') }} AS fm
    ON td.run_id = fm.run_id
    -- Model runs are specific to townships
    AND CONTAINS(fm.township_code_coverage, td.meta_township_code)
