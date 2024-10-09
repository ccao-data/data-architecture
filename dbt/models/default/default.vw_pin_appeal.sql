-- View containing appeals by PIN
WITH reasons AS (
    SELECT
        htpar.*,
        -- Reason codes come from different columns before and after 2020
        CASE
            WHEN htpar.taxyr <= '2020'
                AND TRIM(SUBSTR(htpar.user42, 1, 2)) NOT IN ('0', ':')
                THEN TRIM(SUBSTR(htpar.user42, 1, 2))
            WHEN htpar.taxyr > '2020' THEN htpar.user89
        END AS reason_code1,
        CASE
            WHEN htpar.taxyr <= '2020'
                AND TRIM(SUBSTR(htpar.user43, 1, 2)) NOT IN ('0', ':')
                THEN TRIM(SUBSTR(htpar.user42, 1, 2))
            WHEN htpar.taxyr > '2020' THEN htpar.user100
        END AS reason_code2,
        CASE
            WHEN htpar.taxyr <= '2020'
                AND TRIM(SUBSTR(htpar.user44, 1, 2)) NOT IN ('0', ':')
                THEN TRIM(SUBSTR(htpar.user42, 1, 2))
            WHEN htpar.taxyr > '2020' THEN htpar.user101
        END AS reason_code3
    FROM {{ source('iasworld', 'htpar') }} AS htpar
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

    -- Status and agent name come from different columns before and after 2020
    CASE
        WHEN reasons.taxyr <= '2020' AND reasons.resact = 'C' THEN 'change'
        WHEN reasons.taxyr <= '2020' AND reasons.resact = 'NC' THEN 'no change'
        WHEN
            reasons.taxyr > '2020' AND TRIM(LOWER(reasons.user104)) = 'decrease'
            THEN 'change'
        WHEN reasons.taxyr > '2020' THEN TRIM(LOWER(reasons.user104))
    END AS change,
    reasons.reason_code1,
    reascd1.description AS reason1,
    reasons.reason_code2,
    reascd2.description AS reason2,
    reasons.reason_code3,
    reascd3.description AS reason3,
    reasons.cpatty AS agent_code,
    htagnt.name1 AS agent_name,
    CASE
        WHEN reasons.hrstatus = 'C' THEN 'closed'
        WHEN reasons.hrstatus = 'O' THEN 'open'
        WHEN reasons.hrstatus = 'P' THEN 'pending'
        WHEN reasons.hrstatus = 'X' THEN 'closed pending c of e'
    END AS status
FROM reasons
LEFT JOIN {{ source('iasworld', 'pardat') }} AS pardat
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
WHERE reasons.cur = 'Y'
    AND reasons.caseno IS NOT NULL
    AND reasons.deactivat IS NULL
