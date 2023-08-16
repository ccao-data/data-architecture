-- View containing current and prior years' assessments by PIN in wide format
-- CCAO mailed_tot, CCAO final, and BOR final values for each PIN by year
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
FROM {{ ref('iasworld.asmt_all') }}
WHERE procname IN ('CCAOVALUE', 'CCAOFINAL', 'BORVALUE')
    AND rolltype != 'RR'
    AND deactivat IS NULL
    AND valclass IS NULL
GROUP BY parid, taxyr
