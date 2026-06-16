SELECT
    oby.parid,
    oby.taxyr,
    legdat.user1 AS township_code,
    oby.class
-- Source PINs from PARDAT to filter out any records in subsequent tables that
-- may represent deactivated parcels
FROM {{ source('iasworld', 'pardat') }} AS pardat
INNER JOIN {{ source('iasworld', 'oby') }} AS oby
    ON pardat.parid = oby.parid
    AND pardat.taxyr = oby.taxyr
    AND oby.cur = 'Y'
    AND oby.deactivat IS NULL
LEFT JOIN {{ source('iasworld', 'legdat') }} AS legdat
    ON oby.parid = legdat.parid
    AND oby.taxyr = legdat.taxyr
    AND legdat.cur = 'Y'
    AND legdat.deactivat IS NULL
WHERE pardat.cur = 'Y'
    AND pardat.deactivat IS NULL
    AND oby.class = '289'
