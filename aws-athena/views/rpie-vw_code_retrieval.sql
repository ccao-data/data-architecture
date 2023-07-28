-- View containing RPIE codes and mailing addresses
CREATE OR REPLACE VIEW rpie.vw_code_retrieval AS
SELECT
    pin_codes.pin,
    pin_codes.year,
    pin_codes.rpie_code,
    pardat.class,
    owndat.own1 AS mailing_name,
    owndat.addr1 AS mailing_addr1,
    CONCAT_WS(
        ' ',
        CONCAT_WS(', ', owndat.cityname, owndat.statecode),
        CONCAT_WS('-', owndat.zip1, owndat.zip2)
    ) AS mailing_addr2
FROM rpie.pin_codes
LEFT JOIN iasworld.owndat
    ON pin_codes.pin = owndat.parid
    AND pin_codes.year = owndat.taxyr
LEFT JOIN iasworld.pardat
    ON pin_codes.pin = pardat.parid
    AND pin_codes.year = pardat.taxyr
