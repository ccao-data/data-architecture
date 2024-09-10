/* This CTE creates our AHSAP indicator. It's a function of multiple columns
from multiple iasWorld tables having a variety of values. We group by parid and
taxyr, and use MAX() since most of these tables are NOT unique by parcel and
year while the view is. Therefore, if any part of a parcel (card, lline, etc.)
triggers AHSAP status, the parcel as a whole will be identified as AHSAP (since
TRUE > FALSE). user columns with "AI" prefixes that trigger AHSAP status are
'alternative CDU' and user columns with "SAP" prefixes that trigger AHSAP status
are 'incentive number'. comdat.user13 is 'subclass 2'.

For an explanation of AHSAP and insight into why it involves so many different
iasWorld tables, see: https://www.cookcountyassessor.com/affordable-housing */
WITH ahsap AS (
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
            OR SUBSTR(dwel.user16, 1, 2) = 'AI'
            OR admn.excode = 'INCV', FALSE
        )) AS is_ahsap
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
    LEFT JOIN {{ source('iasworld', 'exadmn') }} AS admn
        ON par.parid = admn.parid
        AND par.taxyr = admn.taxyr
        AND admn.cur = 'Y'
        AND admn.deactivat IS NULL
    WHERE par.cur = 'Y'
        AND par.deactivat IS NULL
    GROUP BY par.parid, par.taxyr
),

-- These CTEs make it easier to work with CDUs from oby, comdat, pardat since
-- those tables aren't unique by parid and taxyr.
dwel_cdu AS (
    SELECT
        dwel.parid,
        dwel.taxyr,
        ARRAY_JOIN(
            ARRAY_SORT(ARRAY_DISTINCT(ARRAY_AGG(dwel.cdu))), ', '
        ) AS cdu
    FROM {{ source('iasworld', 'dweldat') }} AS dwel
    WHERE dwel.cdu IS NOT NULL
        AND dwel.cur = 'Y'
        AND dwel.deactivat IS NULL
    GROUP BY dwel.parid, dwel.taxyr
),

com_cdu AS (
    SELECT
        com.parid,
        com.taxyr,
        ARRAY_JOIN(
            ARRAY_SORT(ARRAY_DISTINCT(ARRAY_AGG(com.user16))), ', '
        ) AS cdu
    FROM {{ source('iasworld', 'comdat') }} AS com
    WHERE com.user16 IS NOT NULL
        AND com.cur = 'Y'
        AND com.deactivat IS NULL
    GROUP BY com.parid, com.taxyr
),

oby_cdu AS (
    SELECT
        oby.parid,
        oby.taxyr,
        ARRAY_JOIN(
            ARRAY_SORT(ARRAY_DISTINCT(ARRAY_AGG(oby.user16))), ', '
        ) AS cdu
    FROM {{ source('iasworld', 'oby') }} AS oby
    WHERE oby.user16 IS NOT NULL
        AND oby.cur = 'Y'
        AND oby.deactivat IS NULL
    GROUP BY oby.parid, oby.taxyr
)

SELECT
    pdat.parid AS pin,
    pdat.class,
    pdat.taxyr AS year,
    colo.is_corner_lot,
    ahsap.is_ahsap,
    COALESCE(vpe.pin IS NOT NULL, FALSE) AS is_exempt,
    COALESCE(pin.tax_bill_total = 0, FALSE) AS is_zero_bill,
    vpcc.is_parking_space,
    vpcc.parking_space_flag_reason,
    vpcc.is_common_area,
    SUBSTR(pdat.parid, 11, 1) = '8' AS is_leasehold,
    ptst.test_type AS weirdness,
    oby.cdu AS oby_cdu,
    cdat.cdu AS com_cdu,
    ddat.cdu AS dwel_cdu,
    pdat.note2 AS note
FROM {{ source('iasworld', 'pardat') }} AS pdat
LEFT JOIN {{ source('spatial', 'corner') }} AS colo
    ON SUBSTR(pdat.parid, 1, 10) = colo.pin10
    AND pdat.taxyr = colo.year
LEFT JOIN ahsap
    ON pdat.parid = ahsap.parid
    AND pdat.taxyr = ahsap.taxyr
LEFT JOIN {{ ref('default.vw_pin_exempt') }} AS vpe
    ON pdat.parid = vpe.pin
    AND pdat.taxyr = vpe.year
LEFT JOIN {{ source('tax', 'pin') }} AS pin
    ON pdat.parid = pin.pin
    AND pdat.taxyr = pin.year
LEFT JOIN {{ ref('default.vw_pin_condo_char') }} AS vpcc
    ON pdat.parid = vpcc.pin
    AND pdat.taxyr = vpcc.year
LEFT JOIN oby_cdu AS oby
    ON pdat.parid = oby.parid
    AND pdat.taxyr = oby.taxyr
LEFT JOIN com_cdu AS cdat
    ON pdat.parid = cdat.parid
    AND pdat.taxyr = cdat.taxyr
LEFT JOIN dwel_cdu AS ddat
    ON pdat.parid = ddat.parid
    AND pdat.taxyr = ddat.taxyr
LEFT JOIN {{ ref('ccao.pin_test') }} AS ptst
    ON pdat.parid = ptst.pin
    AND pdat.taxyr = ptst.year
WHERE pdat.cur = 'Y'
    AND pdat.deactivat IS NULL
