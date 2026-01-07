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
WHERE valasm2 = 0
    AND class NOT IN (
        '100', '200', '239', '240', '241', '300', '400', '500', '550', '637',
        '637A', '637B', '650', '651', '651A', '651B', '700', '700A', '700B',
        '742', '742A', '742B', '800', '800A', '800B', '850', '850A', '850B',
        '900', 'EX', 'RR', '999', '535'
    )
