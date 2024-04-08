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
        WHEN SUBSTR(class, 1, 2) IN ('EX', 'RR') THEN class
        -- OA classes contain their major class as the third and final
        -- character
        WHEN SUBSTR(class, 1, 2) = 'OA' THEN SUBSTR(class, 3, 1)
        WHEN class IN (
                '500', '535', '501', '516', '517', '522', '523',
                '526', '527', '528', '529', '530', '531', '532',
                '533', '535', '590', '591', '592', '597', '599'
            ) THEN '5A'
        WHEN class IN (
                '550', '580', '581', '583', '587', '589', '593'
            ) THEN '5B'
        ELSE SUBSTR(class, 1, 1)
    END AS major_class,
    CASE WHEN class IN ('299', '399') THEN 'CONDO'
        WHEN class IN ('211', '212') THEN 'MF'
        WHEN
            class IN (
                '202', '203', '204', '205', '206', '207',
                '208', '209', '210', '234', '278', '295'
            )
            THEN 'SF'
    END AS property_group,
    CASE
        WHEN procname = 'CCAOVALUE' THEN 'MAILED'
        WHEN procname = 'CCAOFINAL' THEN 'ASSESSOR CERTIFIED'
        WHEN procname = 'BORVALUE' THEN 'BOR CERTIFIED'
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
