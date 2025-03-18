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
WHERE valasm2 != 0
    -- Filter for vacant classes
    AND class IN (
        '100', '200', '239', '240', '241', '300', '400', '500', '550', '637',
        '637A', '637B', '650', '651', '651A', '651B', '700', '700A', '700B',
        '742', '742A', '742B', '800', '800A', '800B', '850', '850A', '850B',
        '900', 'EX', 'RR', '999'
    )
