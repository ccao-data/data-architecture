SELECT
    pardat.taxyr,
    pardat.parid,
    pardat.class,
    legdat.user1 AS township_code,
    aprval.aprbldg,
    aprval.aprland,
    aprval.aprtot,
    aprval.revcode,
    aprval.reascd AS reason_for_change,
    aprval.wen,
    aprval.who
FROM {{ source('iasworld', 'pardat') }} AS pardat
LEFT JOIN {{ source('iasworld', 'legdat') }} AS legdat
    ON pardat.taxyr = legdat.taxyr
    AND pardat.parid = legdat.parid
LEFT JOIN {{ source('iasworld', 'aprval') }} AS aprval
    ON pardat.taxyr = aprval.taxyr
    AND pardat.parid = aprval.parid
WHERE pardat.cur = 'Y'
    AND pardat.deactivat IS NULL
    AND legdat.cur = 'Y'
    AND legdat.deactivat IS NULL
    AND aprval.cur = 'Y'
    AND aprval.deactivat IS NULL
