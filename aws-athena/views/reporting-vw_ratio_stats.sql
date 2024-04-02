-- View containing ratios by pin, intended to feed the
-- glue job 'reporting-ratio_stats'.

WITH towns AS (
    SELECT
        leg.parid AS pin,
        leg.taxyr AS year,
        towns.township_name,
        towns.township_code,
        towns.triad_code AS triad
    FROM {{ source('iasworld', 'legdat') }} AS leg
    LEFT JOIN {{ source('spatial', 'township') }} AS towns
        ON leg.user1 = towns.township_code
    WHERE leg.cur = 'Y'
        AND leg.deactivat IS NULL
),

-- Model values for corresponding triads only
model_values AS (
    SELECT
        ap.meta_pin AS pin,
        CAST(CAST(ap.meta_year AS INT) + 1 AS VARCHAR) AS year,
        ap.meta_class AS class,
        'model' AS assessment_stage,
        ap.pred_pin_final_fmv_round AS total
    FROM {{ source('model', 'assessment_pin') }} AS ap
    INNER JOIN {{ source('model', 'final_model') }} AS final_model
        ON ap.run_id = final_model.run_id
    -- Model runs are specific to townships
    WHERE final_model.township_code_coverage LIKE CONCAT(
            '%', ap.township_code, '%'
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
    CASE WHEN av.class IN ('299', '399') THEN 'CONDO'
        WHEN av.class IN ('211', '212') THEN 'MF'
        WHEN
            av.class IN (
                '202', '203', '204', '205', '206', '207',
                '208', '209', '210', '234', '278', '295'
            )
            THEN 'SF'
    END AS property_group,
    av.assessment_stage,
    towns.triad,
    towns.township_code,
    av.total AS fmv,
    vwps.sale_price,
    av.total / vwps.sale_price AS ratio
FROM {{ ref('default.vw_pin_sale') }} AS vwps
LEFT JOIN towns
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
    AND av.class IN (
        '299', '399', '211', '212', '202', '203', '204', '205', '206', '207',
        '208', '209', '210', '234', '278', '295'
    )
    AND ps.pin IS NULL
