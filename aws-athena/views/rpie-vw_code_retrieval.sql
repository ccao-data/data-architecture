-- View containing RPIE codes and mailing addresses
CREATE OR REPLACE VIEW rpie.vw_code_retrieval AS
WITH address AS (
    SELECT
        parid AS pin,
        own1 AS mailing_name,
        addr1 AS mailing_addr1,
        CONCAT_WS(
            ' ',
            CONCAT_WS(', ', cityname, statecode),
            CONCAT_WS('-', zip1, zip2)
        ) AS mailing_addr2,
        taxyr AS year
    FROM iasworld.owndat
)

SELECT
    pc.pin,
    pc.year,
    pc.rpie_code,
    pardat.class,
    ad.mailing_name,
    ad.mailing_addr1,
    ad.mailing_addr2
FROM rpie.pin_codes AS pc
LEFT JOIN address AS ad
    ON pc.pin = ad.pin
    AND pc.year = ad.year
LEFT JOIN iasworld.pardat
    ON pc.pin = pardat.parid
    AND pc.year = pardat.taxyr
