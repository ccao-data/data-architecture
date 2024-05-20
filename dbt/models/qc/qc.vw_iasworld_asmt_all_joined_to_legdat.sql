SELECT
    asmt_all.*,
    legdat.user1 AS township_code
FROM {{ source('iasworld', 'asmt_all') }} AS asmt_all
LEFT JOIN {{ source('iasworld', 'legdat') }} AS legdat
    ON asmt_all.taxyr = legdat.taxyr
    AND asmt_all.parid = legdat.parid
    AND legdat.cur = 'Y'
    AND legdat.deactivat IS NULL
WHERE asmt_all.rolltype != 'RR'
    AND asmt_all.procname IN ('CCAOVALUE', 'CCAOFINAL', 'BORVALUE')
    AND asmt_all.deactivat IS NULL
    AND asmt_all.valclass IS NULL
