 -- View containing RPIE codes and mailing addresses
CREATE OR replace VIEW rpie.vw_code_retrieval
AS
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
    TAXYR AS year
  FROM iasworld.owndat
)

SELECT
    PC.pin,
    PC.year,
    PC.rpie_code,
    pardat.class,
    AD.mailing_name,
    AD.mailing_addr1,
    AD.mailing_addr2
FROM rpie.pin_codes PC
LEFT JOIN address AD ON PC.pin = AD.pin AND PC.year = AD.year
LEFT JOIN iasworld.pardat ON PC.pin = pardat.parid AND PC.year = pardat.taxyr