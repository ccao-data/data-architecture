-- View containing values from each stage of assessment by PIN and year
CREATE OR REPLACE VIEW default.vw_pin_value AS
WITH stage_values AS (
    SELECT
        parid AS pin,
        taxyr AS year,
        -- Mailed values
        CASE
            WHEN procname = 'CCAOVALUE' AND taxyr < '2020' THEN ovrvalasm2
            WHEN procname = 'CCAOVALUE' AND taxyr >= '2020' THEN valasm2
        END AS mailed_bldg,
        CASE
            WHEN procname = 'CCAOVALUE' AND taxyr < '2020' THEN ovrvalasm1
            WHEN procname = 'CCAOVALUE' AND taxyr >= '2020' THEN valasm1
        END AS mailed_land,
        CASE
            WHEN procname = 'CCAOVALUE' AND taxyr < '2020' THEN ovrvalasm3
            WHEN procname = 'CCAOVALUE' AND taxyr >= '2020' THEN valasm3
        END AS mailed_tot,
        -- Assessor certified values
        CASE
            WHEN procname = 'CCAOFINAL' AND taxyr < '2020' THEN ovrvalasm2
            WHEN procname = 'CCAOFINAL' AND taxyr >= '2020' THEN valasm2
        END AS certified_bldg,
        CASE
            WHEN procname = 'CCAOFINAL' AND taxyr < '2020' THEN ovrvalasm1
            WHEN procname = 'CCAOFINAL' AND taxyr >= '2020' THEN valasm1
        END AS certified_land,
        CASE
            WHEN procname = 'CCAOFINAL' AND taxyr < '2020' THEN ovrvalasm3
            WHEN procname = 'CCAOFINAL' AND taxyr >= '2020' THEN valasm3
        END AS certified_tot,
        -- Board certified values
        CASE
            WHEN procname = 'BORVALUE' AND taxyr < '2020' THEN ovrvalasm2
            WHEN procname = 'BORVALUE' AND taxyr >= '2020' THEN valasm2
        END AS board_bldg,
        CASE
            WHEN procname = 'BORVALUE' AND taxyr < '2020' THEN ovrvalasm1
            WHEN procname = 'BORVALUE' AND taxyr >= '2020' THEN valasm1
        END AS board_land,
        CASE
            WHEN procname = 'BORVALUE' AND taxyr < '2020' THEN ovrvalasm3
            WHEN procname = 'BORVALUE' AND taxyr >= '2020' THEN valasm3
        END AS board_tot
    FROM iasworld.asmt_all
    WHERE procname IN ('CCAOVALUE', 'CCAOFINAL', 'BORVALUE')
        AND cur = 'Y'
        AND rolltype != 'RR'
        AND deactivat IS NULL
        AND valclass IS NULL
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
