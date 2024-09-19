-- View containing values from each stage of assessment by PIN and year
-- in wide format

-- CCAO mailed, CCAO final, and BOR final values for each PIN by year.
-- We use ARBITRARY functions here for two reasons: 1) To flatten three stages
-- of assessment into one row, and 2) to deduplicate PINs with multiple rows for
-- a given stage/pin/year combination. Values are always the same within these
-- duplicates.
WITH stage_values AS (
    SELECT
        parid AS pin,
        taxyr AS year,
        -- Mailed values
        ARBITRARY(
            CASE
                WHEN
                    procname = 'CCAOVALUE'
                    THEN REGEXP_REPLACE(class, '[^[:alnum:]]', '')
            END
        ) AS mailed_class,
        ARBITRARY(
            CASE
                WHEN procname = 'CCAOVALUE' AND taxyr < '2020' THEN ovrvalasm2
                WHEN procname = 'CCAOVALUE' AND taxyr >= '2020' THEN valasm2
            END
        ) AS mailed_bldg,
        ARBITRARY(
            CASE
                WHEN procname = 'CCAOVALUE' AND taxyr < '2020' THEN ovrvalasm1
                WHEN procname = 'CCAOVALUE' AND taxyr >= '2020' THEN valasm1
            END
        ) AS mailed_land,
        ARBITRARY(
            CASE
                WHEN procname = 'CCAOVALUE' AND taxyr < '2020' THEN ovrvalasm3
                WHEN procname = 'CCAOVALUE' AND taxyr >= '2020' THEN valasm3
            END
        ) AS mailed_tot,
        -- Mailed market values
        ARBITRARY(
            CASE
                WHEN procname = 'CCAOVALUE' AND taxyr < '2020' THEN NULL
                WHEN procname = 'CCAOVALUE' AND taxyr >= '2020' THEN valapr2
            END
        ) AS mailed_bldg_mv,
        ARBITRARY(
            CASE
                WHEN procname = 'CCAOVALUE' AND taxyr < '2020' THEN NULL
                WHEN procname = 'CCAOVALUE' AND taxyr >= '2020' THEN valapr1
            END
        ) AS mailed_land_mv,
        ARBITRARY(
            CASE
                WHEN procname = 'CCAOVALUE' AND taxyr < '2020' THEN NULL
                WHEN procname = 'CCAOVALUE' AND taxyr >= '2020' THEN valapr3
            END
        ) AS mailed_tot_mv,
        -- Assessor certified values
        ARBITRARY(
            CASE
                WHEN
                    procname = 'CCAOFINAL'
                    THEN REGEXP_REPLACE(class, '[^[:alnum:]]', '')
            END
        ) AS certified_class,
        ARBITRARY(
            CASE
                WHEN procname = 'CCAOFINAL' AND taxyr < '2020' THEN ovrvalasm2
                WHEN procname = 'CCAOFINAL' AND taxyr >= '2020' THEN valasm2
            END
        ) AS certified_bldg,
        ARBITRARY(
            CASE
                WHEN procname = 'CCAOFINAL' AND taxyr < '2020' THEN ovrvalasm1
                WHEN procname = 'CCAOFINAL' AND taxyr >= '2020' THEN valasm1
            END
        ) AS certified_land,
        ARBITRARY(
            CASE
                WHEN procname = 'CCAOFINAL' AND taxyr < '2020' THEN ovrvalasm3
                WHEN procname = 'CCAOFINAL' AND taxyr >= '2020' THEN valasm3
            END
        ) AS certified_tot,
        -- Assessor certified market values
        ARBITRARY(
            CASE
                WHEN procname = 'CCAOFINAL' AND taxyr < '2020' THEN NULL
                WHEN procname = 'CCAOFINAL' AND taxyr >= '2020' THEN valapr2
            END
        ) AS certified_bldg_mv,
        ARBITRARY(
            CASE
                WHEN procname = 'CCAOFINAL' AND taxyr < '2020' THEN NULL
                WHEN procname = 'CCAOFINAL' AND taxyr >= '2020' THEN valapr1
            END
        ) AS certified_land_mv,
        ARBITRARY(
            CASE
                WHEN procname = 'CCAOFINAL' AND taxyr < '2020' THEN NULL
                WHEN procname = 'CCAOFINAL' AND taxyr >= '2020' THEN valapr3
            END
        ) AS certified_tot_mv,
        -- Board certified values
        ARBITRARY(
            CASE
                WHEN
                    procname = 'BORVALUE'
                    THEN REGEXP_REPLACE(class, '[^[:alnum:]]', '')
            END
        ) AS board_class,
        ARBITRARY(
            CASE
                WHEN procname = 'BORVALUE' AND taxyr < '2020' THEN ovrvalasm2
                WHEN procname = 'BORVALUE' AND taxyr >= '2020' THEN valasm2
            END
        ) AS board_bldg,
        ARBITRARY(
            CASE
                WHEN procname = 'BORVALUE' AND taxyr < '2020' THEN ovrvalasm1
                WHEN procname = 'BORVALUE' AND taxyr >= '2020' THEN valasm1
            END
        ) AS board_land,
        ARBITRARY(
            CASE
                WHEN procname = 'BORVALUE' AND taxyr < '2020' THEN ovrvalasm3
                WHEN procname = 'BORVALUE' AND taxyr >= '2020' THEN valasm3
            END
        ) AS board_tot,
        -- Board certified market values
        ARBITRARY(
            CASE
                WHEN procname = 'BORVALUE' AND taxyr < '2020' THEN NULL
                WHEN procname = 'BORVALUE' AND taxyr >= '2020' THEN valapr2
            END
        ) AS board_bldg_mv,
        ARBITRARY(
            CASE
                WHEN procname = 'BORVALUE' AND taxyr < '2020' THEN NULL
                WHEN procname = 'BORVALUE' AND taxyr >= '2020' THEN valapr1
            END
        ) AS board_land_mv,
        ARBITRARY(
            CASE
                WHEN procname = 'BORVALUE' AND taxyr < '2020' THEN NULL
                WHEN procname = 'BORVALUE' AND taxyr >= '2020' THEN valapr3
            END
        ) AS board_tot_mv
    FROM {{ source('iasworld', 'asmt_all') }}
    WHERE procname IN ('CCAOVALUE', 'CCAOFINAL', 'BORVALUE')
        AND rolltype != 'RR'
        AND deactivat IS NULL
        AND valclass IS NULL
    GROUP BY parid, taxyr
),

clean_values AS (
    SELECT
        stage_values.*,
        -- Current stage indicator
        CASE
            WHEN stage_values.board_tot IS NOT NULL THEN 'BOARD CERTIFIED'
            WHEN
                stage_values.certified_tot IS NOT NULL
                THEN 'ASSESSOR CERTIFIED'
            WHEN stage_values.mailed_tot IS NOT NULL THEN 'MAILED'
        END AS stage_name,
        CASE
            WHEN stage_values.board_tot IS NOT NULL THEN 3
            WHEN stage_values.certified_tot IS NOT NULL THEN 2
            WHEN stage_values.mailed_tot IS NOT NULL THEN 1
        END AS stage_num
    FROM stage_values
),

change_reasons AS (
    SELECT
        parid AS pin,
        taxyr AS year,
        reascd,
        CASE
            WHEN procname = 'CCAOVALUE' THEN 'MAILED'
            WHEN procname = 'CCAOFINAL' THEN 'ASSESSOR CERTIFIED'
            WHEN procname = 'BORVALUE' THEN 'BOARD CERTIFIED'
        END AS stage_name
    FROM {{ source('iasworld', 'aprval') }}
    WHERE reascd IS NOT NULL
        AND procname IS NOT NULL
)

SELECT
    vals.*,
    descr.description AS change_reason
FROM clean_values AS vals
LEFT JOIN change_reasons AS reasons
    ON vals.pin = reasons.pin
    AND vals.year = reasons.year
    AND vals.stage_name = reasons.stage_name
LEFT JOIN {{ ref('ccao.aprval_reascd') }} AS descr
    ON reasons.reascd = descr.reascd
