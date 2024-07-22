SELECT
    legdat.parid,
    legdat.taxyr,
    legdat.user1 AS township_code,
    pardat.class,
    owndat.own1,
    aprval.reascd,
    aprval.who,
    comdat.card,
    comdat.chgrsn,
    comdat_prev.external_occpct AS external_occpct_prev,
    comdat.external_occpct,
    comdat.mktadj,
    comdat.external_occpct
    - comdat_prev.external_occpct AS external_occpct_difference,
    comdat_prev.bldgval AS bldgval_prev,
    comdat.bldgval,
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
FROM {{ source('iasworld', 'comdat') }} AS comdat
-- Join to the prior year of comdat data to pull prior year market value
LEFT JOIN {{ source('iasworld', 'comdat') }} AS comdat_prev
    ON comdat.parid = comdat_prev.parid
    AND comdat.card = comdat_prev.card
    AND CAST(comdat.taxyr AS INT) = CAST(comdat_prev.taxyr AS INT) + 1
    AND comdat_prev.cur = 'Y'
    AND comdat_prev.deactivat IS NULL
LEFT JOIN {{ source('iasworld', 'asmt_all') }} AS asmt
    ON comdat.parid = asmt.parid
    AND comdat.taxyr = asmt.taxyr
    AND asmt.cur = 'Y'
    AND asmt.deactivat IS NULL
    AND asmt.valclass IS NULL
-- Join to the prior year of asmt data to pull the prior assessed values
LEFT JOIN {{ source('iasworld', 'asmt_all') }} AS asmt_prev
    ON asmt.parid = asmt_prev.parid
    AND CAST(asmt.taxyr AS INT) = CAST(asmt_prev.taxyr AS INT) + 1
    AND asmt_prev.cur = 'Y'
    AND asmt_prev.deactivat IS NULL
    AND asmt_prev.valclass IS NULL
LEFT JOIN {{ source('iasworld', 'legdat') }} AS legdat
    ON comdat.parid = legdat.parid
    AND comdat.taxyr = legdat.taxyr
    AND legdat.cur = 'Y'
    AND legdat.deactivat IS NULL
LEFT JOIN {{ source('iasworld', 'pardat') }} AS pardat
    ON comdat.parid = pardat.parid
    AND comdat.taxyr = pardat.taxyr
    AND pardat.cur = 'Y'
    AND pardat.deactivat IS NULL
LEFT JOIN {{ source('iasworld', 'owndat') }} AS owndat
    ON comdat.parid = owndat.parid
    AND comdat.taxyr = owndat.taxyr
    AND owndat.cur = 'Y'
    AND owndat.deactivat IS NULL
LEFT JOIN {{ source('iasworld', 'aprval') }} AS aprval
    ON comdat.parid = aprval.parid
    AND comdat.taxyr = aprval.taxyr
    AND aprval.cur = 'Y'
    AND aprval.deactivat IS NULL
WHERE comdat.cur = 'Y'
    AND comdat.deactivat IS NULL
    AND comdat.chgrsn IN ('5', '5B')
