 -- View containing appeals by PIN.
 -- Appeal values are not yet in iasWorld, so this view cannot be completed
CREATE OR REPLACE VIEW default.vw_pin_appeal AS
  -- CCAO mailed_tot and CCAO final values for each PIN by year
WITH values_by_year AS (
    SELECT
        parid,
        taxyr,
        -- Mailed values
        Max(CASE
            WHEN procname = 'CCAOVALUE'
                    AND taxyr < '2020' THEN ovrvalasm2
            WHEN procname = 'CCAOVALUE'
                    AND taxyr >= '2020'
                    AND valclass IS NULL THEN valasm2
            ELSE NULL
            END) AS mailed_bldg,
        Max(CASE
            WHEN procname = 'CCAOVALUE'
                    AND taxyr < '2020' THEN ovrvalasm1
            WHEN procname = 'CCAOVALUE'
                    AND taxyr >= '2020'
                    AND valclass IS NULL THEN valasm1
            ELSE NULL
            END) AS mailed_land,
        Max(CASE
            WHEN procname = 'CCAOVALUE'
                    AND taxyr < '2020' THEN ovrvalasm3
            WHEN procname = 'CCAOVALUE'
                    AND taxyr >= '2020'
                    AND valclass IS NULL THEN valasm3
            ELSE NULL
            END) AS mailed_tot,
        -- Assessor certified values
        Max(CASE
            WHEN procname = 'CCAOFINAL'
                    AND taxyr < '2020' THEN ovrvalasm2
            WHEN procname = 'CCAOFINAL'
                    AND taxyr >= '2020'
                    AND valclass IS NULL THEN valasm2
            ELSE NULL
            END) AS certified_bldg,
        Max(CASE
            WHEN procname = 'CCAOFINAL'
                    AND taxyr < '2020' THEN ovrvalasm1
            WHEN procname = 'CCAOFINAL'
                    AND taxyr >= '2020'
                    AND valclass IS NULL THEN valasm1
            ELSE NULL
            END) AS certified_land,
        Max(CASE
            WHEN procname = 'CCAOFINAL'
                    AND taxyr < '2020' THEN ovrvalasm3
            WHEN procname = 'CCAOFINAL'
                    AND taxyr >= '2020'
                    AND valclass IS NULL THEN valasm3
            ELSE NULL
            END) AS certified_tot
    FROM iasworld.asmt_all
    WHERE procname IN ('CCAOVALUE', 'CCAOFINAL', 'BORVALUE')
    GROUP BY parid, taxyr
)
SELECT
    htpar.parid AS pin,
    pardat.class AS class,
    legdat.user1 AS township_code,
    htpar.taxyr AS year,
    values_by_year.mailed_bldg,
    values_by_year.mailed_land,
    values_by_year.mailed_tot,
    values_by_year.certified_bldg,
    values_by_year.certified_land,
    values_by_year.certified_tot,
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
LEFT JOIN values_by_year
    ON htpar.parid = values_by_year.parid
    AND htpar.taxyr = values_by_year.taxyr
LEFT JOIN iasworld.htagnt
    ON htpar.cpatty = htagnt.agent
WHERE htpar.cur = 'Y'
AND htpar.caseno IS NOT NULL
