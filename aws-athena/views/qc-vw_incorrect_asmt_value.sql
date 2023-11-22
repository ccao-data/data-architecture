SELECT
    pardat.taxyr,
    legdat.user1 AS township_code,
    pardat.parid,
    pardat.class,
    pardat.luc,
    owndat.own1,
    aprval.reascd AS reason_for_change,
    aprval.who,
    asmt_all.valapr1 AS pri_year_lmv,
    asmt_all.valapr2 AS pri_year_bmv,
    asmt_all.valapr3 AS pri_year_fmv,
    asmt_all.valasm1 AS pri_year_lav,
    asmt_all.valasm2 AS pri_year_bav,
    asmt_all.valasm3 AS pri_year_fav,
    asmt.valapr1 AS cur_year_lmv,
    asmt.valapr2 AS cur_year_bmv,
    asmt.valapr3 AS cur_year_fmv,
    asmt.valasm1 AS cur_year_lav,
    asmt.valasm2 AS cur_year_bav,
    asmt.valasm3 AS cur_year_fav
FROM {{ source('iasworld', 'pardat') }} AS pardat
LEFT JOIN {{ source('iasworld', 'legdat') }} AS legdat
    ON pardat.taxyr = legdat.taxyr
    AND pardat.parid = legdat.parid
LEFT JOIN {{ source('iasworld', 'aprval') }} AS aprval
    ON pardat.taxyr = aprval.taxyr
    AND pardat.parid = aprval.parid
LEFT JOIN {{ source('iasworld', 'owndat') }} AS owndat
    ON pardat.taxyr = owndat.taxyr
    AND pardat.parid = owndat.parid
LEFT JOIN (
    -- This is intended to replicate the ASMT table since we don't
    -- actually have access to it and ASMT_ALL is just the union of
    -- ASMT and ASMT_HIST
    SELECT *
    FROM {{ source('iasworld', 'asmt_all') }}
    EXCEPT
    SELECT *
    FROM {{ source('iasworld', 'asmt_hist') }}
) AS asmt
    ON legdat.taxyr = asmt.taxyr
    AND legdat.parid = asmt.parid
LEFT JOIN {{ source('iasworld', 'asmt_all') }} AS asmt_all
    -- Join the prior year's ASMT_ALL values
    ON asmt.taxyr = CAST(CAST(asmt_all.taxyr AS INT) + 1 AS VARCHAR)
    AND asmt.parid = asmt_all.parid
    AND asmt.rolltype = asmt_all.rolltype
WHERE pardat.cur = 'Y'
    AND pardat.deactivat IS NULL
    AND legdat.cur = 'Y'
    AND legdat.deactivat IS NULL
    AND aprval.cur = 'Y'
    AND aprval.deactivat IS NULL
    AND owndat.cur = 'Y'
    AND asmt.cur = 'Y'
    AND asmt.valclass IS NULL
    AND asmt_all.cur = 'Y'
    AND asmt_all.valclass IS NULL
