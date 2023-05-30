-- View containing ratios by pin, intended to feed glue job 'reporting-ratio_stats'.
CREATE OR REPLACE VIEW reporting.vw_ratio_stats AS
-- Valuation class from pardat
WITH classes AS (
    SELECT
        parid,
        taxyr,
        class,
        CASE WHEN class IN ('299', '399') THEN 'CONDO'
            WHEN class IN ('211', '212') THEN 'MF'
            WHEN
                class IN (
                    '202', '203', '204', '205', '206', '207',
                    '208', '209', '210', '234', '278', '295'
                )
                THEN 'SF'
            ELSE NULL
        END AS property_group
    FROM iasworld.pardat
),

townships AS (
    SELECT
        parid,
        taxyr,
        SUBSTR(taxdist, 1, 2) AS township_code,
        triad_code AS triad
    FROM iasworld.legdat
    LEFT JOIN spatial.township
        ON SUBSTR(taxdist, 1, 2) = township_code
),

model_values AS (
    SELECT
        meta_pin AS parid,
        CAST(CAST(meta_year AS INT) + 1 AS VARCHAR) AS year,
        'model' AS assessment_stage,
        pred_pin_final_fmv_round AS total
    FROM model.assessment_pin
    LEFT JOIN classes
        ON assessment_pin.meta_pin = classes.parid
        AND assessment_pin.meta_year = classes.taxyr
    LEFT JOIN townships
        ON assessment_pin.meta_pin = townships.parid
        AND assessment_pin.meta_year = townships.taxyr
    WHERE run_id IN (SELECT run_id FROM model.final_model)
        AND property_group IS NOT NULL
),

iasworld_values AS (
    SELECT
        asmt_all.parid,
        asmt_all.taxyr AS year,
        CASE
            WHEN procname = 'CCAOVALUE' THEN 'mailed'
            WHEN procname = 'CCAOFINAL' THEN 'assessor certified'
            WHEN procname = 'BORVALUE' THEN 'bor certified'
            ELSE NULL
        END AS assessment_stage,
        MAX(
            CASE
                WHEN asmt_all.taxyr < '2020' THEN ovrvalasm3
                WHEN asmt_all.taxyr >= '2020' THEN valasm3
                ELSE NULL
            END
        ) * 10 AS total
    FROM iasworld.asmt_all
    WHERE (valclass IS NULL OR asmt_all.taxyr < '2020')
        AND procname IN ('CCAOVALUE', 'CCAOFINAL', 'BORVALUE')
        AND asmt_all.taxyr >= '2021'
    GROUP BY
        asmt_all.parid,
        asmt_all.taxyr,
        procname,
        CASE
            WHEN procname = 'CCAOVALUE' THEN 'mailed'
            WHEN procname = 'CCAOFINAL' THEN 'assessor certified'
            WHEN procname = 'BORVALUE' THEN 'bor certified'
            ELSE NULL
        END
),

all_values AS (
    SELECT * FROM model_values
    UNION
    SELECT * FROM iasworld_values
),

ps AS (
    SELECT * FROM default.vw_pin_condo_char WHERE is_parking_space = TRUE
)

SELECT
    vps.pin,
    av.year,
    vps.year AS sale_year,
    property_group,
    assessment_stage,
    triad,
    townships.township_code,
    av.total AS fmv,
    sale_price,
    av.total / sale_price AS ratio
FROM default.vw_pin_sale AS vps
LEFT JOIN classes
    ON vps.pin = classes.parid AND vps.year = classes.taxyr
LEFT JOIN townships
    ON vps.pin = townships.parid AND vps.year = townships.taxyr
    -- Join sales so that values for a given year can be compared to a
    -- complete set of sales from the previous year
INNER JOIN all_values AS av
    ON vps.pin = av.parid
    AND CAST(vps.year AS INT) = CAST(av.year AS INT) - 1
    -- Grab parking spaces and join them to aggregate stats for removal
LEFT JOIN ps ON av.parid = ps.pin AND av.year = ps.year
WHERE is_multisale = FALSE
    AND property_group IS NOT NULL
    AND ps.pin IS NULL
