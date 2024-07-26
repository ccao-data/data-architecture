SELECT
    pardat.parid,
    pardat.taxyr,
    legdat.user1 AS township_code,
    pardat.class,
    aprval.revcode,
    aprval.reascd AS reason_for_change,
    aprval.aprland,
    aprval.aprbldg,
    aprval.aprtot,
    aprval.who,
    DATE_FORMAT(
        DATE_PARSE(aprval.wen, '%Y-%m-%d %H:%i:%S.%f'), '%c/%e/%Y %H:%i'
    ) AS wen
FROM {{ source('iasworld', 'aprval') }} AS aprval
LEFT JOIN {{ source('iasworld', 'pardat') }} AS pardat
    ON aprval.parid = pardat.parid
    AND aprval.taxyr = pardat.taxyr
    AND pardat.cur = 'Y'
    AND pardat.deactivat IS NULL
LEFT JOIN {{ source('iasworld', 'legdat') }} AS legdat
    ON aprval.parid = legdat.parid
    AND aprval.taxyr = legdat.taxyr
    AND legdat.cur = 'Y'
    AND legdat.deactivat IS NULL
WHERE aprval.cur = 'Y'
    AND aprval.deactivat IS NULL
    -- Filter for residential parcels not set to cost approach
    AND pardat.class LIKE '2%'
    AND (aprval.revcode IS NULL OR aprval.revcode != '1')
