-- View containing appeals by PIN

-- CTE so that we can join reason descriptions onto cleaned reason codes and
-- drop some dupes from htpar.
WITH reasons AS (
    -- Select distinct rows to deduplicate a few appeals that got published
    -- twice by accident, otherwise we wouldn't expect any dupes in this query
    SELECT DISTINCT
        htpar.parid,
        htpar.taxyr,
        htpar.caseno,
        htpar.user38,
        htpar.user19,
        htpar.user20,
        htpar.user21,
        htpar.user22,
        htpar.user23,
        htpar.user24,
        htpar.user25,
        htpar.user109,
        htpar.user110,
        htpar.user40,
        htpar.user81,
        htpar.user104,
        htpar.resact,
        htpar.hrstatus,
        htpar.cpatty,
        -- user125 isn't used in the view, but it's FilingID and we want to
        -- include it to make sure we're only dropping true dupes using DISTINCT
        htpar.user125,
        -- Reason codes come from different columns before and after 2020
        CASE
            WHEN htpar.taxyr <= '2020'
                AND TRIM(SUBSTR(htpar.user42, 1, 2)) NOT IN (
                    '0', ':'
                )
                THEN TRIM(SUBSTR(htpar.user42, 1, 2))
            WHEN
                htpar.taxyr > '2020'
                THEN REGEXP_REPLACE(htpar.user89, '[^[:alnum:]]', '')
        END AS reason_code1,
        CASE
            WHEN htpar.taxyr <= '2020'
                AND TRIM(SUBSTR(htpar.user43, 1, 2)) NOT IN (
                    '0', ':'
                )
                THEN TRIM(SUBSTR(htpar.user43, 1, 2))
            WHEN
                htpar.taxyr > '2020'
                THEN REGEXP_REPLACE(htpar.user100, '[^[:alnum:]]', '')
        END AS reason_code2,
        CASE
            WHEN htpar.taxyr <= '2020'
                AND TRIM(SUBSTR(htpar.user44, 1, 2)) NOT IN (
                    '0', ':'
                )
                THEN TRIM(SUBSTR(htpar.user44, 1, 2))
            WHEN
                htpar.taxyr > '2020'
                THEN REGEXP_REPLACE(htpar.user101, '[^[:alnum:]]', '')
        END AS reason_code3
    FROM {{ source('iasworld', 'htpar') }} AS htpar
    WHERE htpar.cur = 'Y'
        AND htpar.deactivat IS NULL
        AND htpar.caseno IS NOT NULL
        AND (
            htpar.heartyp = 'A'
            -- Remove legacy COEs
            OR (htpar.heartyp = 'C' AND SUBSTR(htpar.caseno, 1, 3) != 'COE')
        )

)

SELECT
    reasons.parid AS pin,
    REGEXP_REPLACE(pardat.class, '[^[:alnum:]]', '') AS class,
    legdat.user1 AS township_code,
    reasons.taxyr AS year,
    vwpv.mailed_bldg,
    vwpv.mailed_land,
    vwpv.mailed_tot,
    vwpv.certified_bldg,
    vwpv.certified_land,
    vwpv.certified_tot,
    reasons.caseno AS case_no,
    CASE
        WHEN reasons.user38 = 'CC'
            OR (
                reasons.user38 IN ('RS', 'IC')
                AND REGEXP_REPLACE(pardat.class, '[^[:alnum:]]', '') IN (
                    '213', '297', '299', '399', '599'
                )
            ) THEN 'condo/coop'
        WHEN reasons.user38 = 'CE' THEN 'c of e - exempt'
        WHEN reasons.user38 = 'CI' THEN 'c of e - incentive'
        WHEN reasons.user38 = 'CO' THEN 'c of e - omitted'
        WHEN reasons.user38 = 'CV' THEN 'c of e - valuations'
        WHEN reasons.user38 = 'IC' THEN 'commercial'
        WHEN reasons.user38 = 'IN' THEN 'incentive'
        WHEN reasons.user38 = 'LD' THEN 'land'
        WHEN reasons.user38 = 'OM' THEN 'omitted assessment'
        WHEN reasons.user38 = 'RS' THEN 'residential'
    END AS appeal_type,

    CASE
        WHEN reasons.user19 = 'Y' THEN TRUE WHEN reasons.user19 = 'N' THEN FALSE
    END AS reason_lack_of_uniformity,
    CASE
        WHEN reasons.user20 = 'Y' THEN TRUE WHEN reasons.user20 = 'N' THEN FALSE
    END AS reason_fire_damage,
    CASE
        WHEN reasons.user21 = 'Y' THEN TRUE WHEN reasons.user21 = 'N' THEN FALSE
    END AS reason_building_no_longer_exists,
    CASE
        WHEN reasons.user22 = 'Y' THEN TRUE WHEN reasons.user22 = 'N' THEN FALSE
    END AS reason_over_valuation,
    CASE
        WHEN reasons.user23 = 'Y' THEN TRUE WHEN reasons.user23 = 'N' THEN FALSE
    END AS reason_property_description_error,
    CASE
        WHEN reasons.user24 = 'Y' THEN TRUE WHEN reasons.user24 = 'N' THEN FALSE
    END AS reason_vacancy_occupancy,
    CASE
        WHEN reasons.user25 = 'Y' THEN TRUE WHEN reasons.user25 = 'N' THEN FALSE
    END AS reason_building_is_uninhabitable,
    CASE
        WHEN reasons.user109 = 'Y' THEN TRUE WHEN
            reasons.user109 = 'N'
            THEN FALSE
    END AS reason_homeowner_exemption,
    CASE
        WHEN reasons.user110 = 'Y' THEN TRUE WHEN
            reasons.user110 = 'N'
            THEN FALSE
    END AS reason_legal_argument,
    CASE
        WHEN reasons.user40 = 'Y' THEN TRUE WHEN reasons.user40 = 'N' THEN FALSE
    END AS reason_other,
    reasons.user81 AS reason_other_description,

    -- Status and agent name come from different columns before and after 2020
    CASE
        WHEN reasons.taxyr <= '2020' AND reasons.resact = 'C' THEN 'change'
        WHEN reasons.taxyr <= '2020' AND reasons.resact = 'NC' THEN 'no change'
        WHEN
            reasons.taxyr > '2020' AND TRIM(LOWER(reasons.user104)) = 'decrease'
            THEN 'change'
        WHEN
            reasons.taxyr > '2020'
            AND TRIM(LOWER(reasons.user104)) IN ('change', 'no change')
            THEN TRIM(LOWER(reasons.user104))
    END AS change,
    reasons.reason_code1,
    reascd1.description AS reason_desc1,
    reasons.reason_code2,
    reascd2.description AS reason_desc2,
    reasons.reason_code3,
    reascd3.description AS reason_desc3,
    reasons.cpatty AS agent_code,
    htagnt.name1 AS agent_name,
    CASE
        WHEN reasons.hrstatus = 'C' THEN 'closed'
        WHEN reasons.hrstatus = 'O' THEN 'open'
        WHEN reasons.hrstatus = 'P' THEN 'pending'
        WHEN reasons.hrstatus = 'X' THEN 'closed pending c of e'
    END AS status
FROM reasons
-- This is an INNER JOIN since there are apparently parcels in htpar that are
-- not in pardat. See 31051000511064, 2008.
INNER JOIN {{ source('iasworld', 'pardat') }} AS pardat
    ON reasons.parid = pardat.parid
    AND reasons.taxyr = pardat.taxyr
    AND pardat.cur = 'Y'
    AND pardat.deactivat IS NULL
LEFT JOIN {{ source('iasworld', 'legdat') }} AS legdat
    ON reasons.parid = legdat.parid
    AND reasons.taxyr = legdat.taxyr
    AND legdat.cur = 'Y'
    AND legdat.deactivat IS NULL
LEFT JOIN {{ ref('default.vw_pin_value') }} AS vwpv
    ON reasons.parid = vwpv.pin
    AND reasons.taxyr = vwpv.year
LEFT JOIN {{ source('iasworld', 'htagnt') }} AS htagnt
    ON reasons.cpatty = htagnt.agent
    AND htagnt.cur = 'Y'
    AND htagnt.deactivat IS NULL
LEFT JOIN {{ ref('ccao.htpar_reascd') }} AS reascd1
    ON reasons.reason_code1 = reascd1.reascd
LEFT JOIN {{ ref('ccao.htpar_reascd') }} AS reascd2
    ON reasons.reason_code2 = reascd2.reascd
LEFT JOIN {{ ref('ccao.htpar_reascd') }} AS reascd3
    ON reasons.reason_code3 = reascd3.reascd
