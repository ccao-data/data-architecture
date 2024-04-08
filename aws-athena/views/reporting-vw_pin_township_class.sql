-- This view provides common grouping columns used across many other reporting
-- views and CTAS.
WITH correct_class AS (
    SELECT
        parid,
        taxyr,
        REGEXP_REPLACE(class, '([^0-9EXR])', '') AS class,
        nbhd,
        deactivat,
        cur
    FROM {{ source('iasworld', 'pardat') }}
)

SELECT
    correct.parid AS pin,
    correct.taxyr AS year,
    town.triad_name,
    town.triad_code,
    town.township_name,
    leg.user1 AS township_code,
    SUBSTR(correct.nbhd, 3, 3) AS nbhd,
    correct.class,
    CASE
        WHEN SUBSTR(correct.class, 1, 2) IN ('EX', 'RR') THEN correct.class
        -- OA classes contain their major class as the third and final
        -- character
        WHEN SUBSTR(correct.class, 1, 2) = 'OA' THEN SUBSTR(correct.class, 3, 1)
        WHEN correct.class IN (
                '500', '535', '501', '516', '517', '522', '523',
                '526', '527', '528', '529', '530', '531', '532',
                '533', '535', '590', '591', '592', '597', '599'
            ) THEN '5A'
        WHEN correct.class IN (
                '550', '580', '581', '583', '587', '589', '593'
            ) THEN '5B'
        ELSE SUBSTR(correct.class, 1, 1)
    END AS major_class,
    CASE WHEN correct.class IN ('299', '399') THEN 'CONDO'
        WHEN correct.class IN ('211', '212') THEN 'MF'
        WHEN
            correct.class IN (
                '202', '203', '204', '205', '206', '207',
                '208', '209', '210', '234', '278', '295'
            )
            THEN 'SF'
    END AS property_group,
    CASE
        WHEN
            MOD(CAST(values_by_year.year AS INT), 3) = 0
            AND town.triad_name = 'North'
            THEN TRUE
        WHEN
            MOD(CAST(values_by_year.year AS INT), 3) = 1
            AND town.triad_name = 'South'
            THEN TRUE
        WHEN
            MOD(CAST(values_by_year.year AS INT), 3) = 2
            AND town.triad_name = 'City'
            THEN TRUE
        ELSE FALSE
    END AS reassessment_year,
    CAST(CAST(correct.taxyr AS INT) + 1 AS VARCHAR) AS model_year
FROM correct_class AS correct
LEFT JOIN {{ source('iasworld', 'legdat') }} AS leg
    ON correct.parid = leg.parid
    AND correct.taxyr = leg.taxyr
LEFT JOIN {{ source('spatial', 'township') }} AS town
    ON leg.user1 = town.township_code
WHERE correct.cur = 'Y'
    AND correct.deactivat IS NULL
    AND leg.cur = 'Y'
    AND leg.deactivat IS NULL
