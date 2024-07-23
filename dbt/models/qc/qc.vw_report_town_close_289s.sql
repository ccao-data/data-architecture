SELECT
    oby.parid,
    oby.taxyr,
    legdat.user1 AS township_code,
    oby.class
FROM {{ source('iasworld', 'oby') }} AS oby
LEFT JOIN {{ source('iasworld', 'legdat') }} AS legdat
    ON oby.parid = legdat.parid
    AND oby.taxyr = legdat.taxyr
    AND legdat.cur = 'Y'
    AND legdat.deactivat IS NULL
WHERE oby.cur = 'Y'
    AND oby.deactivat IS NULL
    AND oby.class = '289'
