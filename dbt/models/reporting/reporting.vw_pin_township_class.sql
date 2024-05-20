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
),

/* This CTE creates our AHSAP indicator. It's a function of multiple columns
from multiple iasWorld tables having a variety of values. We group by parid and
taxyr, and use MAX() since most of these tables are NOT unique by parcel and
year while the view is. Therefore, if any part of a parcel (card, lline, etc.)
triggers AHSAP status, the parcel as a whole will be identified as AHSAP (since
TRUE > FALSE). user columns with "AI" prefixes that trigger AHSAP status are
'alternative CDU' and user columns with "SAP" prefixes that trigger AHSAP status
are 'incentive number'. comdat.user13 is 'subclass 2'. */
ahsap AS (
    SELECT
        par.parid,
        par.taxyr,
        MAX(COALESCE(
            land.infl1 IN ('30', '31', '32') OR SUBSTR(land.user7, 1, 3) = 'SAP'
            OR SUBSTR(oby.user16, 1, 2) = 'AI'
            OR SUBSTR(oby.user3, 1, 3) = 'SAP'
            OR SUBSTR(com.user16, 1, 2) = 'AI'
            OR SUBSTR(com.user4, 1, 3) = 'SAP'
            OR com.user13 IN ('49', '50', '51')
            OR aprval.ecf IS NOT NULL
            OR SUBSTR(dwel.user16, 1, 2) = 'AI', FALSE
        )) AS ahsap
    FROM {{ source('iasworld', 'pardat') }} AS par
    LEFT JOIN {{ source('iasworld', 'comdat') }} AS com
        ON par.parid = com.parid
        AND par.taxyr = com.taxyr
        AND com.cur = 'Y'
        AND com.deactivat IS NULL
    LEFT JOIN {{ source('iasworld', 'dweldat') }} AS dwel
        ON par.parid = dwel.parid
        AND par.taxyr = dwel.taxyr
        AND dwel.cur = 'Y'
        AND dwel.deactivat IS NULL
    LEFT JOIN {{ source('iasworld', 'oby') }} AS oby
        ON par.parid = oby.parid
        AND par.taxyr = oby.taxyr
        AND oby.cur = 'Y'
        AND oby.deactivat IS NULL
    LEFT JOIN {{ source('iasworld', 'land') }} AS land
        ON par.parid = land.parid
        AND par.taxyr = land.taxyr
        AND land.cur = 'Y'
        AND land.deactivat IS NULL
    LEFT JOIN {{ source('iasworld', 'aprval') }} AS aprval
        ON par.parid = aprval.parid
        AND par.taxyr = aprval.taxyr
        AND aprval.cur = 'Y'
        AND aprval.deactivat IS NULL
    WHERE par.cur = 'Y'
        AND par.deactivat IS NULL
    GROUP BY par.parid, par.taxyr
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
        WHEN ARRAY_JOIN(tax.tax_municipality_name, ', ') = '' THEN NULL ELSE
            ARRAY_JOIN(tax.tax_municipality_name, ', ')
    END AS municipality_name,
    correct.class,
    groups.reporting_class_code AS major_class,
    groups.modeling_group AS property_group,
    ahsap.ahsap,
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
LEFT JOIN ahsap
    ON correct.parid = ahsap.parid
    AND correct.taxyr = ahsap.taxyr
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
-- Tax municipality data lags iasWorld data by a year or two at any given time
LEFT JOIN {{ ref('location.tax') }} AS tax
    ON SUBSTR(correct.parid, 1, 10) = tax.pin10
    AND CASE
        WHEN
            correct.taxyr > (SELECT MAX(year) FROM {{ ref('location.tax') }})
            THEN (SELECT MAX(year) FROM {{ ref('location.tax') }})
        ELSE correct.taxyr
    END = tax.year
WHERE correct.cur = 'Y'
    AND correct.deactivat IS NULL
