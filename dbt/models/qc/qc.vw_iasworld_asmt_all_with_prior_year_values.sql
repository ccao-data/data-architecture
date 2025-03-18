SELECT
    legdat.parid,
    asmt.taxyr,
    legdat.user1 AS township_code,
    pardat.nbhd,
    pardat.class,
    pardat.luc,
    owndat.own1,
    CASE
        WHEN
            aprval.reascd IS NOT NULL AND reascd.description IS NOT NULL
            THEN CONCAT(aprval.reascd, ': ', reascd.description)
        ELSE aprval.reascd
    END AS reascd,
    aprval.who,
    asmt_prev.valapr1 AS valapr1_prev,
    asmt_prev.valapr2 AS valapr2_prev,
    asmt_prev.valapr3 AS valapr3_prev,
    asmt_prev.valasm1 AS valasm1_prev,
    asmt_prev.valasm2 AS valasm2_prev,
    asmt_prev.valasm3 AS valasm3_prev,
    asmt.valapr1,
    asmt.valapr2,
    asmt.valapr3,
    asmt.valasm1,
    asmt.valasm2,
    asmt.valasm3
FROM {{ source('iasworld', 'asmt_all') }} AS asmt
LEFT JOIN {{ source('iasworld', 'asmt_all') }} AS asmt_prev
    ON asmt.parid = asmt_prev.parid
    AND CAST(asmt.taxyr AS INT) = CAST(asmt_prev.taxyr AS INT) + 1
    AND asmt_prev.cur = 'Y'
    AND asmt_prev.deactivat IS NULL
    AND asmt_prev.valclass IS NULL
LEFT JOIN {{ source('iasworld', 'legdat') }} AS legdat
    ON asmt.taxyr = legdat.taxyr
    AND asmt.parid = legdat.parid
    AND legdat.cur = 'Y'
    AND legdat.deactivat IS NULL
LEFT JOIN {{ source('iasworld', 'pardat') }} AS pardat
    ON asmt.taxyr = pardat.taxyr
    AND asmt.parid = pardat.parid
    AND pardat.cur = 'Y'
    AND pardat.deactivat IS NULL
LEFT JOIN {{ source('iasworld', 'owndat') }} AS owndat
    ON asmt.taxyr = owndat.taxyr
    AND asmt.parid = owndat.parid
    AND owndat.cur = 'Y'
    AND owndat.deactivat IS NULL
LEFT JOIN {{ source('iasworld', 'aprval') }} AS aprval
    ON asmt.taxyr = aprval.taxyr
    AND asmt.parid = aprval.parid
    AND aprval.cur = 'Y'
    AND aprval.deactivat IS NULL
LEFT JOIN {{ ref('ccao.aprval_reascd') }} AS reascd
    ON aprval.reascd = reascd.reascd
WHERE asmt.cur = 'Y'
    AND asmt.deactivat IS NULL
    AND asmt.valclass IS NULL
