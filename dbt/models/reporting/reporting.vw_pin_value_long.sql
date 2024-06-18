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
        ) AS tot,
        ARBITRARY(
            CASE
                WHEN taxyr < '2020' THEN NULL
                WHEN taxyr >= '2020' THEN valapr2
            END
        ) AS bldg_mv,
        ARBITRARY(
            CASE
                WHEN taxyr < '2020' THEN NULL
                WHEN taxyr >= '2020' THEN valapr1
            END
        ) AS land_mv,
        ARBITRARY(
            CASE
                WHEN taxyr < '2020' THEN NULL
                WHEN taxyr >= '2020' THEN valapr3
            END
        ) AS tot_mv
    FROM {{ source('iasworld', 'asmt_all') }}
    WHERE procname IN ('CCAOVALUE', 'CCAOFINAL', 'BORVALUE')
        AND rolltype != 'RR'
        AND deactivat IS NULL
        AND valclass IS NULL
    GROUP BY parid, taxyr, procname, REGEXP_REPLACE(class, '[^[:alnum:]]', '')
)

SELECT
    svls.parid AS pin,
    svls.taxyr AS year,
    svls.class,
    groups.reporting_class_code AS major_class,
    groups.modeling_group AS property_group,
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
    svls.tot,
    svls.bldg_mv,
    svls.land_mv,
    svls.tot_mv
FROM stage_values AS svls
-- Exclude classes without a reporting class
INNER JOIN {{ ref('ccao.class_dict') }} AS groups
    ON svls.class = groups.class_code
