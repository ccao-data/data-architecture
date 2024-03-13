-- View containing appeals by PIN
SELECT
    htpar.parid AS pin,
    pardat.class AS class,
    legdat.user1 AS township_code,
    htpar.taxyr AS year,
    vwpv.mailed_bldg,
    vwpv.mailed_land,
    vwpv.mailed_tot,
    vwpv.certified_bldg,
    vwpv.certified_land,
    vwpv.certified_tot,
    htpar.caseno AS case_no,
    CASE
        WHEN htpar.user38 = 'CC'
            OR (
                htpar.user38 IN ('RS', 'IC')
                AND pardat.class IN ('213', '297', '299', '399', '599')
            ) THEN 'condo/coop'
        WHEN htpar.user38 = 'CE' THEN 'c of e - exempt'
        WHEN htpar.user38 = 'CI' THEN 'c of e - incentive'
        WHEN htpar.user38 = 'CO' THEN 'c of e - omitted'
        WHEN htpar.user38 = 'CV' THEN 'c of e - valuations'
        WHEN htpar.user38 = 'IC' THEN 'commercial'
        WHEN htpar.user38 = 'IN' THEN 'incentive'
        WHEN htpar.user38 = 'LD' THEN 'land'
        WHEN htpar.user38 = 'OM' THEN 'omitted assessment'
        WHEN htpar.user38 = 'RS' THEN 'residential'
    END AS appeal_type,

    -- Status, reason codes, and agent name come from different columns
    -- before and after 2020
    CASE
        WHEN htpar.taxyr < '2020' AND htpar.resact = 'C' THEN 'change'
        WHEN htpar.taxyr < '2020' AND htpar.resact = 'NC' THEN 'no change'
        WHEN
            htpar.taxyr >= '2020' AND TRIM(LOWER(htpar.user104)) = 'decrease'
            THEN 'change'
        WHEN htpar.taxyr >= '2020' THEN TRIM(LOWER(htpar.user104))
    END AS change,
    CASE
        WHEN htpar.taxyr < '2020'
            AND TRIM(SUBSTR(htpar.user42, 1, 2)) NOT IN ('0', ':')
            THEN TRIM(SUBSTR(htpar.user42, 1, 2))
        WHEN htpar.taxyr >= '2020' THEN htpar.user89
    END AS reason_code1,
    CASE
        WHEN htpar.taxyr < '2020'
            AND TRIM(SUBSTR(htpar.user43, 1, 2)) NOT IN ('0', ':')
            THEN TRIM(SUBSTR(htpar.user42, 1, 2))
        WHEN htpar.taxyr >= '2020' THEN htpar.user100
    END AS reason_code2,
    CASE
        WHEN htpar.taxyr < '2020'
            AND TRIM(SUBSTR(htpar.user44, 1, 2)) NOT IN ('0', ':')
            THEN TRIM(SUBSTR(htpar.user42, 1, 2))
        WHEN htpar.taxyr >= '2020' THEN htpar.user101
    END AS reason_code3,
    htpar.cpatty AS agent_code,
    htagnt.name1 AS agent_name,
    CASE
        WHEN htpar.hrstatus = 'C' THEN 'closed'
        WHEN htpar.hrstatus = 'O' THEN 'open'
        WHEN htpar.hrstatus = 'P' THEN 'pending'
        WHEN htpar.hrstatus = 'X' THEN 'closed pending c of e'
    END AS status
FROM {{ source('iasworld', 'htpar') }} AS htpar
LEFT JOIN {{ source('iasworld', 'pardat') }} AS pardat
    ON htpar.parid = pardat.parid
    AND htpar.taxyr = pardat.taxyr
    AND pardat.cur = 'Y'
    AND pardat.deactivat IS NULL
LEFT JOIN {{ source('iasworld', 'legdat') }} AS legdat
    ON htpar.parid = legdat.parid
    AND htpar.taxyr = legdat.taxyr
    AND legdat.cur = 'Y'
    AND legdat.deactivat IS NULL
LEFT JOIN {{ ref('default.vw_pin_value') }} AS vwpv
    ON htpar.parid = vwpv.pin
    AND htpar.taxyr = vwpv.year
LEFT JOIN {{ source('iasworld', 'htagnt') }} AS htagnt
    ON htpar.cpatty = htagnt.agent
    AND htagnt.cur = 'Y'
    AND htagnt.deactivat IS NULL
WHERE htpar.cur = 'Y'
    AND htpar.caseno IS NOT NULL
    AND htpar.deactivat IS NULL
    AND pardat.class != '999'
