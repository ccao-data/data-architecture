{{
    config(
        materialized='table'
    )
}}

WITH metadata_filtered AS (
    SELECT
        run_id,
        model_predictor_all_name
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
)

SELECT
    ac.*,
    ap.pred_pin_final_fmv_round,
    ARRAY_JOIN(
        TRANSFORM(
            SPLIT(LOWER(ap.loc_property_address), ' '),
            x -> CONCAT(UPPER(SUBSTR(x, 1, 1)), SUBSTR(x, 2))
        ),
        ' '
    ) AS property_address,
    school.school_elementary_district_name
        AS loc_school_elementary_district_name,
    school.school_secondary_district_name AS loc_school_secondary_district_name,
    meta.model_predictor_all_name
FROM model.assessment_card AS ac
LEFT JOIN model.assessment_pin AS ap
    ON ac.meta_pin = ap.meta_pin
    AND ac.run_id = ap.run_id
LEFT JOIN school_data AS school
    ON SUBSTRING(ac.meta_pin, 1, 10) = school.school_pin
    AND ac.meta_year = school.year
LEFT JOIN metadata_filtered AS meta
    ON ac.run_id = meta.run_id
WHERE ac.run_id IN (SELECT run_id FROM metadata_filtered)
