{{
    config(materialized='table')
}}

SELECT
    fmr.year,
    fmr.run_id,
    CASE fmr.triad_name
        WHEN 'City' THEN '1'
        WHEN 'North' THEN '2'
        WHEN 'South' THEN '3'
    END AS triad_code,
    fmr.type,
    fmr.is_final,
    DATE(DATE_PARSE(fmr.date_chosen, '%c/%e/%Y')) AS date_chosen,
    DATE(DATE_PARSE(fmr.date_emailed, '%c/%e/%Y')) AS date_emailed,
    DATE(DATE_PARSE(fmr.date_finalized, '%c/%e/%Y')) AS date_finalized,
    fmr.reason_chosen,
    CAST(
        JSON_PARSE(fmr.township_code_coverage) AS ARRAY<VARCHAR>
    ) AS township_code_coverage,
    fmr.reference_run_id,
    fmr.email_title,
    fmr.note
FROM {{ ref('model.final_model_raw') }} AS fmr
