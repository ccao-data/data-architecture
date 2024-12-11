SELECT
    pardat.jur,
    pardat.taxyr,
    pardat.parid,
    {{ insert_hyphens('pardat.class', 1) }} AS pardat_class,
    {{ insert_hyphens('pardat.tieback', 2, 4, 7, 10) }} AS tieback,
    legdat.user1 AS township_code,
    oby.card,
    oby.lline,
    {{ insert_hyphens('oby.class', 1) }} AS oby_class,
    oby.ovrrcnld,
    oby.user20 AS proration_percent,
    oby.user16 AS alt_cdu,
    oby.mktadj,
    oby.convoby,
    oby.yrblt,
    oby.chgrsn,
    oby.user18 AS held_market_value,
    oby.external_calc_rcnld,
    oby.external_occpct,
    oby.external_propct,
    oby.external_rcnld,
    oby.calc_meth
FROM {{ source('iasworld', 'oby') }} AS oby
LEFT JOIN {{ source('iasworld', 'legdat') }} AS legdat
    ON oby.parid = legdat.parid
    AND oby.taxyr = legdat.taxyr
    AND legdat.cur = 'Y'
    AND legdat.deactivat IS NULL
LEFT JOIN {{ source('iasworld', 'pardat') }} AS pardat
    ON legdat.parid = pardat.parid
    AND legdat.taxyr = pardat.taxyr
    AND pardat.cur = 'Y'
    AND pardat.deactivat IS NULL
WHERE oby.cur = 'Y'
    AND oby.deactivat IS NULL
    AND pardat.class NOT BETWEEN '200' AND '299'
