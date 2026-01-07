SELECT
    parid AS "PARID",
    taxyr AS "TAXYR",
    township_code AS "TOWNSHIP",
    class AS "CLASS",
    own1 AS "OWN1",
    reascd AS "Reason for Change",
    who AS "WHO",
    valapr1_prev AS "Prior Year LMV",
    valapr2_prev AS "Prior Year BMV",
    valapr3_prev AS "Prior Year Total MV",
    valasm1_prev AS "Prior Year LAV",
    valasm2_prev AS "Prior Year BAV",
    valasm3_prev AS "Prior Year Total AV",
    valapr1 AS "Curr. Year LMV",
    valapr2 AS "Curr. Year BMV",
    valapr3 AS "Curr. Year Total MV",
    valasm1 AS "Curr. Year LAV",
    valasm2 AS "Curr. Year BAV",
    valasm3 AS "Curr. Year Total AV"
FROM {{ ref('qc.vw_iasworld_asmt_all_with_prior_year_values') }}
WHERE valasm3 = 0
    AND class NOT IN ('EX', 'RR')
