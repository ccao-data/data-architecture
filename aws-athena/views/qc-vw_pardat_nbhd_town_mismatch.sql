SELECT
    pardat.taxyr,
    pardat.parid,
    pardat.class,
    pardat.nbhd AS neighborhood_code,
    legdat.user1 AS township_code
FROM {{ source('iasworld', 'pardat') }} AS pardat
LEFT JOIN {{ source('iasworld', 'legdat') }} AS legdat
    ON pardat.taxyr = legdat.taxyr
    AND pardat.parid = legdat.parid
    AND legdat.cur = 'Y'
    AND legdat.deactivat IS NULL
WHERE pardat.cur = 'Y'
    AND pardat.deactivat IS NULL
    AND SUBSTR(pardat.nbhd, 1, 2) != legdat.user1
