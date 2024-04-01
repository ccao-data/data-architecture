-- View containing values from each stage of assessment by PIN and year
-- in long format

-- CCAO mailed, CCAO final, and BOR final values for each PIN by year.
-- We use ARBITRARY functions here to deduplicate PINs with multiple rows for
-- a given stage/pin/year combination. Values are always the same within these
-- duplicates.
WITH stage_values AS (
    SELECT
        parid,
        taxyr,
        REGEXP_REPLACE(class, '[^[:alnum:]]', '') AS class,
        procname,
        ARBITRARY(
            CASE
                WHEN taxyr < '2020' THEN ovrvalasm2
                WHEN taxyr >= '2020' THEN valasm2
            END
        ) AS bldg,
        ARBITRARY(
            CASE
                WHEN taxyr < '2020' THEN ovrvalasm1
                WHEN taxyr >= '2020' THEN valasm1
            END
        ) AS land,
        ARBITRARY(
            CASE
                WHEN taxyr < '2020' THEN ovrvalasm3
                WHEN taxyr >= '2020' THEN valasm3
            END
        ) AS tot
    FROM {{ source('iasworld', 'asmt_all') }}
    WHERE procname IN ('CCAOVALUE', 'CCAOFINAL', 'BORVALUE')
        AND rolltype != 'RR'
        AND deactivat IS NULL
        AND valclass IS NULL
    GROUP BY parid, taxyr, procname, REGEXP_REPLACE(class, '[^[:alnum:]]', '')
)

SELECT
    parid AS pin,
    taxyr AS year,
    class,
    CASE
        WHEN procname = 'CCAOVALUE' THEN 'MAILED'
        WHEN procname = 'CCAOFINAL' THEN 'ASSESSOR CERTIFIED'
        WHEN procname = 'BORVALUE' THEN 'BOARD CERTIFIED'
    END AS stage_name,
    CASE
        WHEN procname = 'CCAOVALUE' THEN 1
        WHEN procname = 'CCAOFINAL' THEN 2
        WHEN procname = 'BORVALUE' THEN 3
    END AS stage_num,
    bldg,
    land,
    tot
FROM stage_values
