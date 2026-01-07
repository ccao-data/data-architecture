SELECT
    oby.parid AS "PARID",
    oby.taxyr AS "TAXYR",
    legdat.user1 AS "TOWNSHIP",
    oby.class AS "CLASS"
FROM {{ source('iasworld', 'oby') }} AS oby
LEFT JOIN {{ source('iasworld', 'legdat') }} AS legdat
    ON oby.parid = legdat.parid
    AND oby.taxyr = legdat.taxyr
    AND legdat.cur = 'Y'
    AND legdat.deactivat IS NULL
WHERE oby.cur = 'Y'
    AND oby.deactivat IS NULL
    AND oby.class = '289'
