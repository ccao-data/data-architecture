-- Table containing ratios by pin, intended to feed the
-- ratio_stats dbt python model
{{
    config(
        materialized='table'
    )
}}

-- Model values for corresponding triads only
WITH model_values AS (
    SELECT
        ap.meta_pin AS pin,
        ap.year,
        ap.meta_class AS class,
        'model' AS assessment_stage,
        ap.pred_pin_final_fmv_round AS total
    FROM {{ source('model', 'assessment_pin') }} AS ap
    INNER JOIN {{ ref('model.final_model') }} AS fm
        ON ap.run_id = fm.run_id
        -- Model runs are specific to townships
        AND (
            -- If reassessment year, use different models for different towns
            (
                CONTAINS(fm.township_code_coverage, ap.township_code)
                AND ap.meta_triad_code = fm.triad_code
            )
            -- Otherwise, just use whichever model is "final"
            OR (ap.meta_triad_code != fm.triad_code AND fm.is_final)
        )
),

-- Values for all stages regardless of triad
iasworld_values AS (
    SELECT
        pin,
        year,
        class,
        LOWER(stage_name) AS assessment_stage,
        tot * 10 AS total
    FROM {{ ref('reporting.vw_pin_value_long') }}
    WHERE year >= '2021'
),

all_values AS (
    SELECT * FROM model_values
    UNION
    SELECT * FROM iasworld_values
)

SELECT
    vwps.pin,
    av.year,
    vwps.year AS sale_year,
    towns.property_group,
    av.assessment_stage,
    towns.triad_code AS triad,
    towns.township_code,
    av.total AS fmv,
    vwps.sale_price,
    av.total / vwps.sale_price AS ratio
FROM {{ ref('default.vw_pin_sale') }} AS vwps
LEFT JOIN {{ ref('reporting.vw_pin_township_class') }} AS towns
    ON vwps.pin = towns.pin
    AND vwps.year = towns.year
    -- Join sales so that values for a given year can be compared to a
    -- complete set of sales from the previous year
INNER JOIN all_values AS av
    ON vwps.pin = av.pin
    AND CAST(vwps.year AS INT) = CAST(av.year AS INT) - 1
-- Grab parking spaces and join them to aggregate stats for removal
LEFT JOIN {{ ref('default.vw_pin_condo_char') }} AS ps
    ON av.pin = ps.pin
    AND av.year = ps.year
    AND ps.is_parking_space = TRUE
WHERE NOT vwps.is_multisale
    AND NOT vwps.sale_filter_deed_type
    AND NOT vwps.sale_filter_less_than_10k
    AND NOT vwps.sale_filter_same_sale_within_365
    AND towns.property_group IS NOT NULL
    AND ps.pin IS NULL
