 -- View containing appeals by PIN.
 -- Appeal values are not yet in iasWorld, so this view cannot be completed
CREATE OR REPLACE VIEW default.vw_pin_appeal AS
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
        WHEN htpar.user38 = 'CC' THEN 'condo/coop'
        WHEN htpar.user38 = 'CE' THEN 'c of e - exempt'
        WHEN htpar.user38 = 'CI' THEN 'c of e - incentive'
        WHEN htpar.user38 = 'CO' THEN 'c of e - omitted'
        WHEN htpar.user38 = 'CV' THEN 'c of e - valuations'
        WHEN htpar.user38 = 'IC' THEN 'commercial'
        WHEN htpar.user38 = 'IN' THEN 'incentive'
        WHEN htpar.user38 = 'LD' THEN 'land'
        WHEN htpar.user38 = 'OM' THEN 'omitteed assessment'
        WHEN htpar.user38 = 'RS' THEN 'residential'
    ELSE NULL END AS appeal_type,
    -- Status, reason codes, and agent name come from different columns before and after 2020
    CASE
        WHEN htpar.taxyr < '2020' AND htpar.resact = 'C' THEN 'change'
        WHEN htpar.taxyr < '2020' AND htpar.resact = 'NC' THEN 'no change'
        WHEN htpar.taxyr >= '2020' THEN lower(htpar.user104)
    ELSE NULL END AS change,
    CASE
        WHEN htpar.taxyr < '2020' AND trim(substr(htpar.user42, 1, 2)) NOT IN ('0', ':')
            THEN trim(substr(htpar.user42, 1, 2))
        WHEN htpar.taxyr >= '2020' THEN htpar.user89
    ELSE NULL END AS reason_code1,
    CASE
        WHEN htpar.taxyr < '2020' AND trim(substr(htpar.user43, 1, 2)) NOT IN ('0', ':')
            THEN trim(substr(htpar.user42, 1, 2))
        WHEN htpar.taxyr >= '2020' THEN htpar.user100
    ELSE NULL END AS reason_code2,
    CASE
        WHEN htpar.taxyr < '2020' AND trim(substr(htpar.user44, 1, 2)) NOT IN ('0', ':')
            THEN trim(substr(htpar.user42, 1, 2))
        WHEN htpar.taxyr >= '2020' THEN htpar.user101
    ELSE NULL END AS reason_code3,
    cpatty AS agent_code,
    htagnt.name1 AS agent_name,
    CASE
        WHEN hrstatus = 'C' THEN 'closed'
        WHEN hrstatus = 'O' THEN 'open'
        WHEN hrstatus = 'P' THEN 'pending'
        WHEN hrstatus = 'X' THEN 'closed pending c of e'
    ELSE NULL END AS status
FROM iasworld.htpar
LEFT JOIN iasworld.pardat
    ON htpar.parid = pardat.parid
    AND htpar.taxyr = pardat.taxyr
LEFT JOIN iasworld.legdat
    ON htpar.parid = legdat.parid
    AND htpar.taxyr = legdat.taxyr
LEFT JOIN default.vw_pin_value vwpv
    ON htpar.parid = vwpv.parid
    AND htpar.taxyr = vwpv.taxyr
LEFT JOIN iasworld.htagnt
    ON htpar.cpatty = htagnt.agent
WHERE htpar.cur = 'Y'
AND htpar.caseno IS NOT NULL
