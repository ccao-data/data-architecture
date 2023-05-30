-- View containing RPIE codes and classes for smartfile
CREATE OR REPLACE VIEW rpie.vw_pin_flatfile AS
WITH classes AS (
    SELECT
        parid AS pin,
        CAST(CAST(taxyr AS INT) + 1 AS VARCHAR) AS year,
        class
    FROM iasworld.pardat
)

SELECT
    pc.pin,
    SUBSTR(pc.pin, 1, 2) || '-'
    || SUBSTR(pc.pin, 3, 2) || '-'
    || SUBSTR(pc.pin, 5, 3) || '-'
    || SUBSTR(pc.pin, 8, 3) || '-'
    || SUBSTR(pc.pin, 11, 4)
        AS pin_pretty,
    pc.year AS rpie_year,
    class,
    rpie_code
FROM (
    SELECT * FROM rpie.pin_codes
    UNION
    SELECT * FROM rpie.pin_codes_dummy
) AS pc
LEFT JOIN classes ON pc.pin = classes.pin AND pc.year = classes.year
