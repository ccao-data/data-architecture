{{
    config(
        materialized='table'
    )
}}

WITH run_ids_to_include AS (
    SELECT run_id
    FROM {{ source('model', 'metadata') }}
    -- This will eventually grab all run_ids where
    -- run_type == comps
    WHERE run_id = '2025-02-11-charming-eric'
)

SELECT
    ac.*,
    ap.pred_pin_final_fmv_round
FROM model.assessment_card AS ac
LEFT JOIN model.assessment_pin AS ap
    ON ac.meta_pin = ap.meta_pin
    AND ac.run_id = ap.run_id
WHERE ac.run_id IN (SELECT run_id FROM run_ids_to_include);
