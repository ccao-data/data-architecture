SELECT
    pardat.taxyr,
    pardat.parid,
    legdat.user1 AS township_code,
    pardat.class AS pardat_class,
    comdat.class AS comdat_class,
    dweldat.class AS dweldat_class,
    oby.class AS oby_class
FROM {{ source('iasworld', 'pardat') }} AS pardat
LEFT JOIN {{ source('iasworld', 'legdat') }} AS legdat
    ON pardat.taxyr = legdat.taxyr
    AND pardat.parid = legdat.parid
    AND legdat.cur = 'Y'
    AND legdat.deactivat IS NULL
LEFT JOIN {{ source('iasworld', 'comdat') }} AS comdat
    ON pardat.taxyr = comdat.taxyr
    AND pardat.parid = comdat.parid
    AND comdat.cur = 'Y'
    AND comdat.deactivat IS NULL
LEFT JOIN {{ source('iasworld', 'dweldat') }} AS dweldat
    ON pardat.taxyr = dweldat.taxyr
    AND pardat.parid = dweldat.parid
    AND dweldat.cur = 'Y'
    AND dweldat.deactivat IS NULL
LEFT JOIN {{ source('iasworld', 'oby') }} AS oby
    ON pardat.taxyr = oby.taxyr
    AND pardat.parid = oby.parid
    AND oby.cur = 'Y'
    AND oby.deactivat IS NULL
WHERE pardat.cur = 'Y'
    AND pardat.deactivat IS NULL
