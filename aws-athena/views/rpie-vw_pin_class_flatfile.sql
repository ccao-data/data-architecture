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
  pin_codes.pin,
  substr(pin_codes.pin, 1, 2) || '-' ||
  substr(pin_codes.pin, 3, 2) || '-' ||
  substr(pin_codes.pin, 5, 3) || '-' ||
  substr(pin_codes.pin, 8, 3) || '-' ||
  substr(pin_codes.pin, 11, 4)
  AS pin_pretty,
  pin_codes.year as rpie_year,
  class,
  rpie_code
FROM rpie.pin_codes

left join classes
    ON pin_codes.pin = classes.pin
    AND pin_codes.year = classes.year