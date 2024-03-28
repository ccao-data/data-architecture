-- View containing ratios by pin, intended to feed the
-- glue job 'reporting-ratio_stats'.

-- Valuation class from pardat
WITH classes AS (
    SELECT
        par.parid,
        par.taxyr,
        par.class,
        CASE WHEN par.class IN ('299', '399') THEN 'CONDO'
            WHEN par.class IN ('211', '212') THEN 'MF'
            WHEN
                par.class IN (
                    '202', '203', '204', '205', '206', '207',
                    '208', '209', '210', '234', '278', '295'
                )
                THEN 'SF'
        END AS property_group
    FROM {{ source('iasworld', 'pardat') }} AS par
    WHERE par.cur = 'Y'
        AND par.deactivat IS NULL
),

townships AS (
    SELECT
        leg.parid,
        leg.taxyr,
        leg.user1 AS township_code
    FROM {{ source('iasworld', 'legdat') }} AS leg
    WHERE leg.cur = 'Y'
        AND leg.deactivat IS NULL
),

town_names AS (
    SELECT
        township_code,
        triad_code AS triad
    FROM {{ source('spatial', 'township') }}
),

model_values AS (
    SELECT
        ap.meta_pin AS parid,
        CAST(CAST(ap.meta_year AS INT) + 1 AS VARCHAR) AS year,
        'model' AS assessment_stage,
        ap.pred_pin_final_fmv_round AS total
    FROM {{ source('model', 'assessment_pin') }} AS ap
    LEFT JOIN classes
        ON ap.meta_pin = classes.parid
        AND ap.meta_year = classes.taxyr
    INNER JOIN {{ ref('model.eph_final_model_long') }} AS fm
        ON ap.run_id = fm.run_id
        AND ap.meta_year = fm.year
        AND ap.township_code = fm.township_code
    WHERE classes.property_group IS NOT NULL
),

iasworld_values AS (
    SELECT
        asmt_all.parid,
        asmt_all.taxyr AS year,
        CASE
            WHEN asmt_all.procname = 'CCAOVALUE' THEN 'mailed'
            WHEN asmt_all.procname = 'CCAOFINAL' THEN 'assessor certified'
            WHEN asmt_all.procname = 'BORVALUE' THEN 'bor certified'
        END AS assessment_stage,
        MAX(
            CASE
                WHEN asmt_all.taxyr < '2020' THEN asmt_all.ovrvalasm3
                WHEN asmt_all.taxyr >= '2020' THEN asmt_all.valasm3
            END
        ) * 10 AS total
    FROM {{ source('iasworld', 'asmt_all') }} AS asmt_all
    WHERE (asmt_all.valclass IS NULL OR asmt_all.taxyr < '2020')
        AND asmt_all.procname IN ('CCAOVALUE', 'CCAOFINAL', 'BORVALUE')
        AND asmt_all.taxyr >= '2021'
        AND asmt_all.deactivat IS NULL
    GROUP BY
        asmt_all.parid,
        asmt_all.taxyr,
        asmt_all.procname,
        CASE
            WHEN asmt_all.procname = 'CCAOVALUE' THEN 'mailed'
            WHEN asmt_all.procname = 'CCAOFINAL' THEN 'assessor certified'
            WHEN asmt_all.procname = 'BORVALUE' THEN 'bor certified'
        END
),

all_values AS (
    SELECT * FROM model_values
    UNION
    SELECT * FROM iasworld_values
),

parking_space AS (
    SELECT *
    FROM {{ ref('default.vw_pin_condo_char') }}
    WHERE is_parking_space = TRUE
)

SELECT
    vwps.pin,
    av.year,
    vwps.year AS sale_year,
    classes.property_group,
    av.assessment_stage,
    town_names.triad,
    townships.township_code,
    av.total AS fmv,
    vwps.sale_price,
    av.total / vwps.sale_price AS ratio
FROM {{ ref('default.vw_pin_sale') }} AS vwps
LEFT JOIN classes
    ON vwps.pin = classes.parid
    AND vwps.year = classes.taxyr
LEFT JOIN townships
    ON vwps.pin = townships.parid
    AND vwps.year = townships.taxyr
LEFT JOIN town_names
    ON townships.township_code = town_names.township_code
    -- Join sales so that values for a given year can be compared to a
    -- complete set of sales from the previous year
INNER JOIN all_values AS av
    ON vwps.pin = av.parid
    AND CAST(vwps.year AS INT) = CAST(av.year AS INT) - 1
    -- Grab parking spaces and join them to aggregate stats for removal
LEFT JOIN parking_space AS ps
    ON av.parid = ps.pin
    AND av.year = ps.year
WHERE NOT vwps.is_multisale
    AND NOT vwps.sale_filter_deed_type
    AND NOT vwps.sale_filter_less_than_10k
    AND NOT vwps.sale_filter_same_sale_within_365
    AND classes.property_group IS NOT NULL
    AND ps.pin IS NULL
