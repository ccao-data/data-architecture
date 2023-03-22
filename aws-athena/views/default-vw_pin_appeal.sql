 -- View containing appeals by PIN.
 -- Appeal values are not yet in iasWorld, so this view cannot be completed.
CREATE OR replace VIEW default.vw_pin_appeal
AS

SELECT
    htpar.parid AS pin,
    pardat.class AS class,
    htpar.user94 AS class_des,
    Substr(legdat.taxdist, 1, 2) AS township,
    htpar.taxyr AS year,
    htpar.caseno AS case_no,
    CASE WHEN htpar.heartyp = '1' THEN 'ASSESSOR RECOMMENDATION'
        WHEN htpar.heartyp = '2' THEN 'CERTIFICATE OF CORRECTION'
        WHEN htpar.heartyp = 'A' THEN 'CURRENT YEAR APPEAL'
        WHEN htpar.heartyp = 'C' THEN 'CURRENT APPEAL & C OF E'
        WHEN htpar.heartyp = 'T' THEN 'TAXABLE'
    ELSE NULL END AS  hearing_type,
    htpar.user38 AS appeal_type,
    htpar.noticval AS notice_val,
    htpar.propreduct AS pre_req_val,
    htpar.user104 AS change,
    htpar.user89 AS reason_code1,
    htpar.user100 AS reason_code2,
    htpar.user101 AS reason_code3,
    resnote1 AS note1,
    resnote2 AS note2

FROM iasworld.htpar
LEFT JOIN iasworld.pardat ON htpar.parid = pardat.parid
    AND htpar.taxyr = pardat.taxyr
LEFT JOIN iasworld.legdat ON htpar.parid = legdat.parid
    AND htpar.taxyr = legdat.taxyr
WHERE htpar.cur = 'Y'