SELECT
    pardat.taxyr,
    pardat.parid,
    pardat.class AS pardat_class,
    legdat.user1 AS township_code,
    comdat.class,
    comdat.who,
    comdat.wen
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
WHERE pardat.cur = 'Y'
    AND pardat.deactivat IS NULL
