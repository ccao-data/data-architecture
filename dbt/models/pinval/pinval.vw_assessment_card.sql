WITH runs_to_include AS (
    SELECT
        run_id,
        model_predictor_all_name,
        assessment_year,
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
)

SELECT
    ac.*,
    ap.pred_pin_final_fmv_round,
    ap.loc_property_address AS property_address,
    elem_sd.name AS school_elementary_district_name,
    sec_sd.name AS school_secondary_district_name,
    loc.tax_municipality_name,
    run.model_predictor_all_name,
    run.assessment_triad,
    run.assessment_year,
    final.final_model_run_date
FROM runs_to_include AS run
INNER JOIN model.assessment_card AS ac
    ON run.run_id = ac.run_id
LEFT JOIN model.assessment_pin AS ap
    ON ac.meta_pin = ap.meta_pin
    AND ac.run_id = ap.run_id
LEFT JOIN spatial.school_district AS elem_sd
    ON ac.loc_school_elementary_district_geoid = elem_sd.geoid
    AND ac.meta_year = elem_sd.year
LEFT JOIN spatial.school_district AS sec_sd
    ON ac.loc_school_secondary_district_geoid = sec_sd.geoid
    AND ac.meta_year = sec_sd.year
LEFT JOIN location.vw_pin10_location AS loc
    ON LEFT(ac.meta_pin, 10) = loc.pin10
    AND ac.meta_year = loc.year
LEFT JOIN final_model_run AS final
    ON run.assessment_year = final.year
WHERE ap.meta_triad_code = final.triad_code;
