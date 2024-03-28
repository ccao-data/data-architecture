-- Gathers AVs by year, major class, assessment stage, and
-- township for reporting
WITH values_by_year AS (
    SELECT
        parid,
        taxyr,
        CASE
            WHEN procname = 'CCAOVALUE' THEN 'mailed'
            WHEN procname = 'CCAOFINAL' THEN 'assessor certified'
            WHEN procname = 'BORVALUE' THEN 'bor certified'
        END AS stage,
        MAX(
            CASE
                WHEN taxyr < '2020' THEN ovrvalasm2
                WHEN taxyr >= '2020' THEN valasm2
            END
        ) AS bldg,
        MAX(
            CASE
                WHEN taxyr < '2020' THEN ovrvalasm1
                WHEN taxyr >= '2020' THEN valasm1
            END
        ) AS land,
        MAX(
            CASE
                WHEN taxyr < '2020' THEN ovrvalasm3
                WHEN taxyr >= '2020' THEN valasm3
            END
        ) AS total
    FROM {{ source('iasworld', 'asmt_all') }}
    WHERE procname IN ('CCAOVALUE', 'CCAOFINAL', 'BORVALUE')
        AND rolltype != 'RR'
        AND deactivat IS NULL
        AND valclass IS NULL
    GROUP BY parid, taxyr, procname
),

-- Add valuation class, townships
classes AS (
    SELECT
        pin AS parid,
        year AS taxyr,
        township_name,
        triad_name AS triad,
        CASE
            WHEN class IN ('EX', 'RR') THEN class
            WHEN class IN (
                    '500', '535', '501', '516', '517', '522', '523',
                    '526', '527', '528', '529', '530', '531', '532',
                    '533', '535', '590', '591', '592', '597', '599'
                ) THEN '5A'
            WHEN class IN (
                    '550', '580', '581', '583', '587', '589', '593'
                ) THEN '5B'
            ELSE SUBSTR(class, 1, 1)
        END AS class
    FROM {{ ref('default.vw_pin_universe') }}
)

-- Add total and median values by township
SELECT
    values_by_year.taxyr AS year,
    values_by_year.stage,
    classes.township_name,
    classes.triad,
    classes.class,
    CASE
        WHEN
            MOD(CAST(values_by_year.taxyr AS INT), 3) = 0
            AND classes.triad = 'North'
            THEN TRUE
        WHEN
            MOD(CAST(values_by_year.taxyr AS INT), 3) = 1
            AND classes.triad = 'South'
            THEN TRUE
        WHEN
            MOD(CAST(values_by_year.taxyr AS INT), 3) = 2
            AND classes.triad = 'City'
            THEN TRUE
        ELSE FALSE
    END AS reassessment_year,
    COUNT(*) AS n,
    SUM(values_by_year.bldg) AS bldg_sum,
    CAST(APPROX_PERCENTILE(values_by_year.bldg, 0.5) AS INT) AS bldg_median,
    SUM(values_by_year.land) AS land_sum,
    CAST(APPROX_PERCENTILE(values_by_year.land, 0.5) AS INT) AS land_median,
    SUM(values_by_year.total) AS tot_sum,
    CAST(APPROX_PERCENTILE(values_by_year.total, 0.5) AS INT) AS tot_median
FROM values_by_year
LEFT JOIN classes
    ON values_by_year.parid = classes.parid
    AND values_by_year.taxyr = classes.taxyr
GROUP BY
    classes.township_name,
    values_by_year.taxyr,
    classes.class,
    classes.triad,
    values_by_year.stage
ORDER BY
    classes.township_name,
    values_by_year.taxyr,
    values_by_year.stage,
    classes.class
