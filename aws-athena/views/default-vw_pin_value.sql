-- View containing values from each stage of assessment by PIN and year
-- in wide format
CREATE OR REPLACE VIEW default.vw_pin_value AS
-- CCAO mailed_tot, CCAO final, and BOR final values for each PIN by year.
-- We use MAX functions here for two reasons: 1) To flatten three stages of
-- assessment into one row, and to deduplicate PINs with multiple rows for
-- a given stage/pin/year combination. Values are always the same within these
-- duplicates.
WITH stage_values AS (
    SELECT
        parid AS pin,
        taxyr AS year,
        -- Mailed values
        MAX(
            CASE
                WHEN procname = 'CCAOVALUE' AND taxyr < '2020' THEN ovrvalasm2
                WHEN procname = 'CCAOVALUE' AND taxyr >= '2020' THEN valasm2
            END
        ) AS mailed_bldg,
        MAX(
            CASE
                WHEN procname = 'CCAOVALUE' AND taxyr < '2020' THEN ovrvalasm1
                WHEN procname = 'CCAOVALUE' AND taxyr >= '2020' THEN valasm1
            END
        ) AS mailed_land,
        MAX(
            CASE
                WHEN procname = 'CCAOVALUE' AND taxyr < '2020' THEN ovrvalasm3
                WHEN procname = 'CCAOVALUE' AND taxyr >= '2020' THEN valasm3
            END
        ) AS mailed_tot,
        -- Assessor certified values
        MAX(
            CASE
                WHEN procname = 'CCAOFINAL' AND taxyr < '2020' THEN ovrvalasm2
                WHEN procname = 'CCAOFINAL' AND taxyr >= '2020' THEN valasm2
            END
        ) AS certified_bldg,
        MAX(
            CASE
                WHEN procname = 'CCAOFINAL' AND taxyr < '2020' THEN ovrvalasm1
                WHEN procname = 'CCAOFINAL' AND taxyr >= '2020' THEN valasm1
            END
        ) AS certified_land,
        MAX(
            CASE
                WHEN procname = 'CCAOFINAL' AND taxyr < '2020' THEN ovrvalasm3
                WHEN procname = 'CCAOFINAL' AND taxyr >= '2020' THEN valasm3
            END
        ) AS certified_tot,
        -- Board certified values
        MAX(
            CASE
                WHEN procname = 'BORVALUE' AND taxyr < '2020' THEN ovrvalasm2
                WHEN procname = 'BORVALUE' AND taxyr >= '2020' THEN valasm2
            END
        ) AS board_bldg,
        MAX(
            CASE
                WHEN procname = 'BORVALUE' AND taxyr < '2020' THEN ovrvalasm1
                WHEN procname = 'BORVALUE' AND taxyr >= '2020' THEN valasm1
            END
        ) AS board_land,
        MAX(
            CASE
                WHEN procname = 'BORVALUE' AND taxyr < '2020' THEN ovrvalasm3
                WHEN procname = 'BORVALUE' AND taxyr >= '2020' THEN valasm3
            END
        ) AS board_tot
    FROM iasworld.asmt_all
    WHERE procname IN ('CCAOVALUE', 'CCAOFINAL', 'BORVALUE')
        AND rolltype != 'RR'
        AND deactivat IS NULL
        AND valclass IS NULL
    GROUP BY parid, taxyr
)

SELECT
    stage_values.*,
    -- Current stage indicator
    CASE
        WHEN stage_values.board_tot IS NOT NULL THEN 'BOARD CERTIFIED'
        WHEN stage_values.certified_tot IS NOT NULL THEN 'ASSESSOR CERTIFIED'
        WHEN stage_values.mailed_tot IS NOT NULL THEN 'MAILED'
    END AS stage_name,
    CASE
        WHEN stage_values.board_tot IS NOT NULL THEN 3
        WHEN stage_values.certified_tot IS NOT NULL THEN 2
        WHEN stage_values.mailed_tot IS NOT NULL THEN 1
    END AS stage_num
FROM stage_values
