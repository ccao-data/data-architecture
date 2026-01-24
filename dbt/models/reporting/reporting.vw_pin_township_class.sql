-- This view provides common grouping columns used across many other reporting
-- views and CTAS.
WITH correct_class AS (
    SELECT
        parid,
        taxyr,
        REGEXP_REPLACE(class, '[^[:alnum:]]', '') AS class,
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
    CASE
        WHEN
            ARRAY_JOIN(vpl.combined_municipality_name, ', ') = ''
            THEN NULL ELSE
            ARRAY_JOIN(vpl.combined_municipality_name, ', ')
    END AS municipality_name,
    correct.class,
    groups.reporting_class_code AS major_class,
    groups.modeling_group AS property_group,
    ahsap.is_ahsap,
    CASE
        WHEN
            MOD(CAST(correct.taxyr AS INT), 3) = 0
            AND town.triad_name = 'North'
            THEN TRUE
        WHEN
            MOD(CAST(correct.taxyr AS INT), 3) = 1
            AND town.triad_name = 'South'
            THEN TRUE
        WHEN
            MOD(CAST(correct.taxyr AS INT), 3) = 2
            AND town.triad_name = 'City'
            THEN TRUE
        ELSE FALSE
    END AS reassessment_year
FROM correct_class AS correct
LEFT JOIN {{ ref('default.vw_pin_status') }} AS ahsap
    ON correct.parid = ahsap.pin
    AND correct.taxyr = ahsap.year
LEFT JOIN {{ source('iasworld', 'legdat') }} AS leg
    ON correct.parid = leg.parid
    AND correct.taxyr = leg.taxyr
    AND leg.cur = 'Y'
    AND leg.deactivat IS NULL
LEFT JOIN {{ source('spatial', 'township') }} AS town
    ON leg.user1 = town.township_code
-- Exclude classes without a reporting class
INNER JOIN {{ ref('ccao.class_dict') }} AS groups
    ON correct.class = groups.class_code
-- Location data in our lake lags iasWorld data by a year or two
LEFT JOIN {{ ref('location.vw_pin10_location') }} AS vpl
    ON SUBSTR(correct.parid, 1, 10) = vpl.pin10
    AND CASE
        WHEN
            correct.taxyr
            > (SELECT MAX(year) FROM {{ ref('location.vw_pin10_location') }})
            THEN (SELECT MAX(year) FROM {{ ref('location.vw_pin10_location') }})
        ELSE correct.taxyr
    END = vpl.year
WHERE correct.cur = 'Y'
    AND correct.deactivat IS NULL
    -- Class 999 are test pins
    AND correct.class NOT IN ('999')
