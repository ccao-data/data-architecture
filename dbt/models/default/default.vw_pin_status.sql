/* This CTE creates our AHSAP indicator. It's a function of multiple columns
from multiple iasWorld tables having a variety of values. We group by parid and
taxyr, and use MAX() since most of these tables are NOT unique by parcel and
year while the view is. Therefore, if any part of a parcel (card, lline, etc.)
triggers AHSAP status, the parcel as a whole will be identified as AHSAP (since
TRUE > FALSE). User columns with "AI" prefixes that trigger AHSAP status are
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
        )) AS is_ahsap,
        /* This column compares classes for parcels across dweldat, comdat, and
        oby in order to construct a mixed use indicator. Note that we do not
        consider EX or vacant land in oby.class to determine whether a parcel is
        residential or commercial. */
        MAX(
            CASE
                /* If a parcel exists in dweldat and comdat, or dweldat and oby
                and class in oby is not vacant land, exempt, or class 2, then
                it's mixed use. */
                WHEN
                    dwel.class IS NOT NULL
                    AND (
                        com.parid IS NOT NULL
                        OR (
                            oby.class IS NOT NULL
                            AND (SUBSTR(oby.class, 1, 1) NOT IN ('1', '2'))
                            AND oby.class NOT IN ('OA2', 'EX')
                        )
                    )
                    THEN TRUE
                    -- If a parcel is class 2 in oby and exists in comdat, then
                    -- it's mixed use.
                WHEN
                    (SUBSTR(oby.class, 1, 1) = '2' OR oby.class = 'OA2')
                    AND com.class IS NOT NULL
                    THEN TRUE
                ELSE FALSE
            END
        ) AS is_mixed_use
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
)

SELECT
    pdat.parid AS pin,
    pdat.taxyr AS year,
    pdat.class,
    CASE
        WHEN
            pdat.taxyr
            >= (SELECT MIN(year) FROM {{ source('spatial', 'corner') }})
            THEN COALESCE(colo.is_corner_lot, FALSE)
    END AS is_corner_lot,
    ahsap.is_ahsap,
    COALESCE(vpe.pin IS NOT NULL, FALSE) AS is_exempt,
    COALESCE(pin.tax_bill_total = 0, NULL) AS is_zero_bill,
    vpcc.is_parking_space,
    vpcc.parking_space_flag_reason,
    vpcc.is_common_area,
    SUBSTR(pdat.parid, 11, 1) = '8' AS is_leasehold,
    ahsap.is_mixed_use,
    pdat.class = 'RR' AS is_railroad,
    ptst.test_type IS NOT NULL AS is_weird,
    ptst.test_type AS weird_flag_reason,
    oby.cdu_code AS oby_cdu_code,
    oby.cdu_description AS oby_cdu_description,
    cdat.cdu_code AS comdat_cdu_code,
    cdat.cdu_description AS comdat_cdu_description,
    ddat.cdu_code AS dweldat_cdu_code,
    ddat.cdu_description AS dweldat_cdu_description,
    pdat.note2 AS pardat_note,
    pdat.class = '999' AS is_filler_class,
    pdat.parid LIKE '%999%' AS is_filler_pin
FROM {{ source('iasworld', 'pardat') }} AS pdat
LEFT JOIN {{ source('spatial', 'corner') }} AS colo
    ON SUBSTR(pdat.parid, 1, 10) = colo.pin10
    /* iasWorld is often a year ahead of the most up-to-date parcel shapefile,
    which our corner lot indicator depends on. This join fills corner lot status
    forward for any years of iasWorld data more recent than the most recent year
    of corner lot data. */
    AND CASE
        WHEN
            pdat.taxyr
            > (SELECT MAX(year) FROM {{ source('spatial', 'corner') }})
            THEN (SELECT MAX(year) FROM {{ source('spatial', 'corner') }})
        ELSE pdat.taxyr
    END
    = colo.year
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
-- CDUs from oby, comdat, dweldat. Since those tables aren't unique by parid
-- and taxyr we use a macro to aggregate them before joining.
LEFT JOIN ({{ aggregate_cdu(
    source_model = source('iasworld', 'oby'),
    cdu_column = 'user16'
    ) }}) AS oby
    ON pdat.parid = oby.parid
    AND pdat.taxyr = oby.taxyr
LEFT JOIN ({{ aggregate_cdu(
    source_model = source('iasworld', 'comdat'),
    cdu_column = 'user16'
    ) }}) AS cdat
    ON pdat.parid = cdat.parid
    AND pdat.taxyr = cdat.taxyr
LEFT JOIN ({{ aggregate_cdu(
    source_model = source('iasworld', 'dweldat'),
    cdu_column = 'user16'
    ) }}) AS ddat
    ON pdat.parid = ddat.parid
    AND pdat.taxyr = ddat.taxyr
LEFT JOIN {{ ref('ccao.pin_test') }} AS ptst
    ON pdat.parid = ptst.pin
    AND pdat.taxyr = ptst.year
WHERE pdat.cur = 'Y'
    AND pdat.deactivat IS NULL
