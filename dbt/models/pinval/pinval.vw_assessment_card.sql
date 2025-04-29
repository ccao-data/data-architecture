WITH runs_to_include AS (
    SELECT
        run_id,
        model_predictor_all_name,
        year
    FROM {{ source('model', 'metadata') }}
    -- This will eventually grab all run_ids where
    -- run_type == comps
    WHERE run_id = '2025-02-11-charming-eric'
),

school_data AS (
    SELECT
        pin10 AS school_pin,
        year,
        school_elementary_district_name,
        school_secondary_district_name
    FROM {{ ref('location.school') }}
    WHERE year > '2014'
),

final_model_run AS (
    SELECT
        year,
        SUBSTRING(run_id, 1, 10) AS final_model_run_date
    FROM model.final_model
    WHERE type = 'res'
        AND is_final
)


SELECT
    ac.*,
    ap.pred_pin_final_fmv_round,
    ap.loc_property_address AS property_address,
    school.school_elementary_district_name
        AS loc_school_elementary_district_name,
    school.school_secondary_district_name AS loc_school_secondary_district_name,
    run.model_predictor_all_name,
    final.final_model_run_date
FROM runs_to_include AS run
INNER JOIN model.assessment_card AS ac
    ON run.run_id = ac.run_id
LEFT JOIN model.assessment_pin AS ap
    ON ac.meta_pin = ap.meta_pin
    AND ac.run_id = ap.run_id
LEFT JOIN school_data AS school
    ON SUBSTRING(ac.meta_pin, 1, 10) = school.school_pin
    AND ac.meta_year = school.year
LEFT JOIN final_model_run AS final
    ON run.year = final.year
