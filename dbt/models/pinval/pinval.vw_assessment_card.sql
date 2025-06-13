WITH runs_to_include AS (
    SELECT
        run_id,
        model_predictor_all_name,
        assessment_year
    FROM {{ source('model', 'metadata') }}
    WHERE run_id = 'comps'
),

school_district AS (
    SELECT
        geoid,
        name,
        year
    FROM {{ source('spatial', 'school_district') }}
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
    sec_sd.name  AS school_secondary_district_name,

    run.model_predictor_all_name,
    final.final_model_run_date
FROM runs_to_include AS run
INNER JOIN model.assessment_card AS ac
    ON run.run_id = ac.run_id
LEFT JOIN model.assessment_pin AS ap
    ON ac.meta_pin = ap.meta_pin
    AND ac.run_id = ap.run_id
LEFT JOIN school_district AS elem_sd
    ON ac.loc_school_elementary_district_geoid = elem_sd.geoid
    AND ac.meta_year = elem_sd.year
LEFT JOIN school_district AS sec_sd
    ON ac.loc_school_secondary_district_geoid = sec_sd.geoid
    AND ac.meta_year = sec_sd.year
LEFT JOIN final_model_run AS final
    ON run.assessment_year = final.year
WHERE ap.meta_triad_code = final.triad_code;
