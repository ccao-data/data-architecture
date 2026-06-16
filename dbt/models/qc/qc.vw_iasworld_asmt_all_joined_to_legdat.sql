SELECT
    asmt_all.*,
    legdat.user1 AS township_code
-- Source PINs from PARDAT to filter out any records in ASMT that may represent
-- deactivated parcels
FROM {{ source('iasworld', 'pardat') }} AS pardat
INNER JOIN {{ source('iasworld', 'asmt_all') }} AS asmt_all
    ON pardat.parid = asmt_all.parid
    AND pardat.taxyr = asmt_all.taxyr
    AND asmt_all.rolltype != 'RR'
    AND asmt_all.procname IN ('CCAOVALUE', 'CCAOFINAL', 'BORVALUE')
    AND asmt_all.deactivat IS NULL
    AND asmt_all.valclass IS NULL
LEFT JOIN {{ source('iasworld', 'legdat') }} AS legdat
    ON asmt_all.taxyr = legdat.taxyr
    AND asmt_all.parid = legdat.parid
    AND legdat.cur = 'Y'
    AND legdat.deactivat IS NULL
WHERE pardat.cur = 'Y'
    AND pardat.deactivat IS NULL
