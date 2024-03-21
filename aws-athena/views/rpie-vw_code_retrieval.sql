-- View containing RPIE codes and mailing addresses
SELECT
    pin_codes.pin,
    pin_codes.year,
    pin_codes.rpie_code,
    REGEXP_REPLACE(pardat.class, '([^0-9EXR])', '') AS class,
    owndat.own1 AS mailing_name,
    owndat.addr1 AS mailing_addr1,
    CONCAT_WS(
        ' ',
        CONCAT_WS(', ', owndat.cityname, owndat.statecode),
        CONCAT_WS('-', owndat.zip1, owndat.zip2)
    ) AS mailing_addr2
FROM {{ source('rpie', 'pin_codes') }} AS pin_codes
LEFT JOIN {{ source('iasworld', 'owndat') }} AS owndat
    ON pin_codes.pin = owndat.parid
    AND pin_codes.year = owndat.taxyr
LEFT JOIN {{ source('iasworld', 'pardat') }} AS pardat
    ON pin_codes.pin = pardat.parid
    AND pin_codes.year = pardat.taxyr
