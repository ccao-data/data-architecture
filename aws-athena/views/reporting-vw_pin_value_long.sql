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
),

classes AS (
    SELECT DISTINCT
        class,
        major_class,
        property_group
    FROM {{ ref('reporting.vw_pin_township_class') }}
)

SELECT
    svls.parid AS pin,
    svls.taxyr AS year,
    svls.class,
    -- Unfortunately there are some classes in asmt_all that are not in pardat.
    -- We need to provide them with a major class through SUBSTR in that case.
    COALESCE(cls.major_class, SUBSTR(svls.class, 1, 1)) AS major_class,
    cls.property_group,
    CASE
        WHEN svls.procname = 'CCAOVALUE' THEN 'MAILED'
        WHEN svls.procname = 'CCAOFINAL' THEN 'ASSESSOR CERTIFIED'
        WHEN svls.procname = 'BORVALUE' THEN 'BOR CERTIFIED'
    END AS stage_name,
    CASE
        WHEN svls.procname = 'CCAOVALUE' THEN 1
        WHEN svls.procname = 'CCAOFINAL' THEN 2
        WHEN svls.procname = 'BORVALUE' THEN 3
    END AS stage_num,
    svls.bldg,
    svls.land,
    svls.tot
FROM stage_values AS svls
LEFT JOIN classes AS cls
    ON svls.class = cls.class