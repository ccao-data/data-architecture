SELECT
    pardat.parid AS "PARID",
    pardat.taxyr AS "TAXYR",
    legdat.user1 AS "TOWNSHIP",
    pardat.class AS "Class",
    aprval.revcode AS "Review Code",
    aprval.reascd AS "Reason for Change",
    aprval.aprland AS "Final Appraised Land Value",
    aprval.aprbldg AS "Final Appraised Building Value",
    aprval.aprtot AS "Final Appr Total Value",
    aprval.who AS "WHO",
    DATE_FORMAT(
        DATE_PARSE(aprval.wen, '%Y-%m-%d %H:%i:%S.%f'), '%c/%e/%Y %H:%i'
    ) AS "WEN"
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
