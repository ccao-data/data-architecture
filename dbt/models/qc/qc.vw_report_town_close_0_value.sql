SELECT
    parid,
    taxyr,
    township_code,
    class,
    own1,
    reascd,
    who,
    valapr1_prev,
    valapr2_prev,
    valapr3_prev,
    valasm1_prev,
    valasm2_prev,
    valasm3_prev,
    valapr1,
    valapr2,
    valapr3,
    valasm1,
    valasm2,
    valasm3
FROM {{ ref('qc.vw_iasworld_asmt_all_with_prior_year_values') }}
WHERE valasm3 = 0
    AND class NOT IN ('EX', 'RR')
