-- Gathers AVs by year, major class, assessment stage, and
-- township for reporting

WITH townships AS (
    SELECT
        legdat.parid AS pin,
        legdat.taxyr AS year,
        township.triad_name AS triad,
        township.township_name
    FROM {{ source('iasworld', 'legdat') }} AS legdat
    LEFT JOIN {{ source('spatial', 'township') }} AS township
        ON legdat.user1 = township.township_code
    WHERE legdat.cur = 'Y'
        AND legdat.deactivat IS NULL
),

-- Classes can change by stage - consolidating them here allows for greater
-- accuracy
stage_classes AS (
    SELECT
        pin,
        year,
        stage_name,
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
    FROM {{ ref('reporting.vw_pin_value_long') }}
)

-- Add total and median values by township
SELECT
    values_by_year.year,
    values_by_year.stage_name AS stage,
    townships.township_name,
    townships.triad,
    stage_classes.class,
    CASE
        WHEN
            MOD(CAST(values_by_year.year AS INT), 3) = 0
            AND townships.triad = 'North'
            THEN TRUE
        WHEN
            MOD(CAST(values_by_year.year AS INT), 3) = 1
            AND townships.triad = 'South'
            THEN TRUE
        WHEN
            MOD(CAST(values_by_year.year AS INT), 3) = 2
            AND townships.triad = 'City'
            THEN TRUE
        ELSE FALSE
    END AS reassessment_year,
    COUNT(*) AS n,
    SUM(values_by_year.bldg) AS bldg_sum,
    CAST(APPROX_PERCENTILE(values_by_year.bldg, 0.5) AS INT) AS bldg_median,
    SUM(values_by_year.land) AS land_sum,
    CAST(APPROX_PERCENTILE(values_by_year.land, 0.5) AS INT) AS land_median,
    SUM(values_by_year.tot) AS tot_sum,
    CAST(APPROX_PERCENTILE(values_by_year.tot, 0.5) AS INT) AS tot_median
FROM {{ ref('reporting.vw_pin_value_long') }} AS values_by_year
LEFT JOIN townships
    ON values_by_year.pin = townships.parid
    AND values_by_year.year = townships.taxyr
LEFT JOIN stage_classes
    ON values_by_year.pin = stage_classes.pin
    AND values_by_year.year = stage_classes.year
    AND values_by_year.stage_name = stage_classes.stage_name
WHERE townships.township_name IS NOT NULL
GROUP BY
    townships.township_name,
    values_by_year.year,
    stage_classes.class,
    townships.triad,
    values_by_year.stage_name
ORDER BY
    townships.township_name,
    values_by_year.year,
    values_by_year.stage_name,
    stage_classes.class
