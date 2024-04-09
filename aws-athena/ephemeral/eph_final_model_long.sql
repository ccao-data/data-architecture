/*
An ephemeral view that takes the nested township codes in model.final_model
and pivots it to create a single row per year, run_id, and township_code
*/
WITH final_model_parsed AS (
    SELECT
        fm.year,
        fm.run_id,
        fm.is_final,
        fm.triad_name,
        CAST(
            JSON_PARSE(fm.township_code_coverage) AS ARRAY<VARCHAR>
        ) AS townships
    FROM {{ ref('model.final_model') }} AS fm
)

SELECT
    final_model_parsed.year,
    final_model_parsed.run_id,
    final_model_parsed.is_final,
    final_model_parsed.triad_name,
    CASE final_model_parsed.triad_name
        WHEN 'City' THEN '1'
        WHEN 'North' THEN '2'
        WHEN 'South' THEN '3'
    END AS triad_code,
    CAST(t.township_code AS VARCHAR) AS township_code
FROM final_model_parsed
CROSS JOIN UNNEST(final_model_parsed.townships) AS t (township_code)
