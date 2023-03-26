-- View containing RPIE codes and classes for smartfile
CREATE OR replace VIEW rpie.vw_pin_flatfile
AS
WITH classes AS (
  SELECT
    parid AS pin,
    Cast(Cast(taxyr AS INT) + 1 AS VARCHAR) AS year,
    class
  FROM iasworld.pardat
  )

SELECT
  pc.pin,
  substr(pc.pin, 1, 2) || '-' ||
  substr(pc.pin, 3, 2) || '-' ||
  substr(pc.pin, 5, 3) || '-' ||
  substr(pc.pin, 8, 3) || '-' ||
  substr(pc.pin, 11, 4)
  AS pin_pretty,
  pc.year as rpie_year,
  class,
  rpie_code
FROM (SELECT * FROM rpie.pin_codes UNION SELECT * FROM rpie.pin_codes_dummy) pc

LEFT JOIN classes ON pc.pin = classes.pin AND pc.year = classes.year
