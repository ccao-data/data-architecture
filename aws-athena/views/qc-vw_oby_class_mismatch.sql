SELECT
    pardat.taxyr,
    pardat.parid,
    SUBSTR(pardat.class, 1, 1) AS pardat_major_class,
    legdat.user1 AS township_code,
    oby.class,
    SUBSTR(oby.class, 1, 1) AS major_class,
    oby.who,
    oby.wen
FROM {{ source('iasworld', 'pardat') }} AS pardat
LEFT JOIN {{ source('iasworld', 'legdat') }} AS legdat
    ON pardat.taxyr = legdat.taxyr
    AND pardat.parid = legdat.parid
    AND legdat.cur = 'Y'
    AND legdat.deactivat IS NULL
LEFT JOIN {{ source('iasworld', 'oby') }} AS oby
    ON pardat.taxyr = oby.taxyr
    AND pardat.parid = oby.parid
    AND oby.cur = 'Y'
    AND oby.deactivat IS NULL
WHERE pardat.cur = 'Y'
    AND pardat.deactivat IS NULL
