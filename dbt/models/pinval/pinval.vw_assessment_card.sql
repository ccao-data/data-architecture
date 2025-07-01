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
),

school_districts AS (
    SELECT
        geoid,
        year,
        MAX(name) AS name
    FROM spatial.school_district
    WHERE geoid IS NOT NULL
    GROUP BY geoid, year
)

SELECT
    ac.*,
    ap.pred_pin_final_fmv_round,
    CAST(
        ROUND(
            ac.pred_card_initial_fmv / NULLIF(ac.char_bldg_sf, 0), 0
        ) AS INTEGER
    )
        AS pred_card_initial_fmv_per_sqft,
    ap.loc_property_address AS property_address,
    CAST(ap.meta_pin_num_cards AS INTEGER) AS meta_pin_num_cards,
    tw.township_name,
    CONCAT(CAST(ac.char_class AS VARCHAR), ': ', cd.class_desc)
        AS char_class_detailed,
    COALESCE(
        CAST(run.assessment_year AS INTEGER) >= 2025
        AND ap.meta_pin_num_cards IN (2, 3), FALSE
    ) AS is_parcel_small_multicard,
    COALESCE(
        CAST(run.assessment_year AS INTEGER) >= 2025
        AND ap.meta_pin_num_cards IN (2, 3)
        AND ROW_NUMBER() OVER (
            PARTITION BY ac.meta_pin, ac.run_id
            ORDER BY COALESCE(ac.char_bldg_sf, 0) DESC, ac.meta_card_num ASC
        ) = 1, FALSE
    ) AS is_frankencard,
    CASE
        WHEN CAST(run.assessment_year AS INTEGER) >= 2025
            AND ap.meta_pin_num_cards IN (2, 3)
            THEN SUM(COALESCE(ac.char_bldg_sf, 0)) OVER (
                PARTITION BY ac.meta_pin, ac.run_id
            )
    END AS combined_bldg_sf,
    elem_sd.name AS school_elementary_district_name,
    sec_sd.name AS school_secondary_district_name,
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
LEFT JOIN school_districts AS elem_sd
    ON ac.loc_school_elementary_district_geoid = elem_sd.geoid
    AND ac.meta_year = elem_sd.year
LEFT JOIN school_districts AS sec_sd
    ON ac.loc_school_secondary_district_geoid = sec_sd.geoid
    AND ac.meta_year = sec_sd.year
LEFT JOIN final_model_run AS final
    ON run.assessment_year = final.year
LEFT JOIN {{ source('spatial', 'township') }} AS tw
    ON ac.township_code = tw.township_code
LEFT JOIN {{ ref('ccao.class_dict') }} AS cd
    ON ac.char_class = cd.class_code
WHERE ap.meta_triad_code = final.triad_code
