 -- View containing current and prior years' assessments by PIN in wide format
CREATE OR replace VIEW default.vw_pin_value
AS
  -- CCAO mailed_tot, CCAO final, and BOR final values for each PIN by year
    SELECT
        parid as pin,
        taxyr as year,
        -- Mailed values
        Max(
          CASE
            WHEN procname = 'CCAOVALUE'
              AND taxyr < '2020' THEN ovrvalasm2
            WHEN procname = 'CCAOVALUE'
              AND taxyr >= '2020' THEN valasm2
            ELSE NULL END
            ) AS mailed_bldg,
        Max(
          CASE
            WHEN procname = 'CCAOVALUE'
              AND taxyr < '2020' THEN ovrvalasm1
            WHEN procname = 'CCAOVALUE'
              AND taxyr >= '2020' THEN valasm1
            ELSE NULL END
            ) AS mailed_land,
        Max(
          CASE
            WHEN procname = 'CCAOVALUE'
              AND taxyr < '2020' THEN ovrvalasm3
            WHEN procname = 'CCAOVALUE'
              AND taxyr >= '2020' THEN valasm3
            ELSE NULL END
            ) AS mailed_tot,
        -- Assessor certified values
        Max(
          CASE
            WHEN procname = 'CCAOFINAL'
              AND taxyr < '2020' THEN ovrvalasm2
            WHEN procname = 'CCAOFINAL'
              AND taxyr >= '2020' THEN valasm2
            ELSE NULL END
            ) AS certified_bldg,
        Max(
          CASE
            WHEN procname = 'CCAOFINAL'
              AND taxyr < '2020' THEN ovrvalasm1
            WHEN procname = 'CCAOFINAL'
              AND taxyr >= '2020' THEN valasm1
            ELSE NULL END
            ) AS certified_land,
        Max(
          CASE
            WHEN procname = 'CCAOFINAL'
              AND taxyr < '2020' THEN ovrvalasm3
            WHEN procname = 'CCAOFINAL'
              AND taxyr >= '2020' THEN valasm3
            ELSE NULL END
            ) AS certified_tot,
        -- Board certified values
        Max(
          CASE
            WHEN procname = 'BORVALUE'
              AND taxyr < '2020' THEN ovrvalasm2
            WHEN procname = 'BORVALUE'
              AND taxyr >= '2020' THEN valasm2
            ELSE NULL END
            ) AS board_bldg,
        Max(
          CASE
            WHEN procname = 'BORVALUE'
              AND taxyr < '2020' THEN ovrvalasm1
            WHEN procname = 'BORVALUE'
              AND taxyr >= '2020' THEN valasm1
            ELSE NULL END
            ) AS board_land,
        Max(
          CASE
            WHEN procname = 'BORVALUE'
              AND taxyr < '2020' THEN ovrvalasm3
            WHEN procname = 'BORVALUE'
              AND taxyr >= '2020' THEN valasm3
            ELSE NULL END
            ) AS board_tot
    FROM iasworld.asmt_all
    WHERE procname IN ('CCAOVALUE', 'CCAOFINAL', 'BORVALUE')
      AND rolltype != 'RR'
      AND deactivat IS NULL
      AND valclass IS NULL
    GROUP BY parid, taxyr
