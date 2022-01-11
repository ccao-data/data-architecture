 -- View containing current and prior years' assessments by PIN in wide format
CREATE OR replace VIEW default.vw_pin_history
AS
  -- CCAO mailed_tot, CCAO final, and BOR final values for each PIN by year
  WITH values_by_year
       AS (SELECT asmt_all.parid,
                  asmt_all.taxyr,
                  Max(CASE
                        WHEN asmt_all.procname = 'CCAOVALUE' AND Cast(asmt_all.taxyr AS INT) < 2020 THEN valasm3
                        WHEN asmt_all.procname = 'CCAOVALUE' AND Cast(asmt_all.taxyr AS INT) = 2020 AND asmt_all.SEQ = 0 THEN valasm3
                        ELSE NULL
                      END) AS mailed_tot,
                  Max(CASE
                        WHEN asmt_all.procname = 'CCAOFINAL' THEN userval2
                        ELSE NULL
                      END) AS certified_bldg,
                  Max(CASE
                        WHEN asmt_all.procname = 'CCAOFINAL' THEN userval1
                        ELSE NULL
                      END) AS certified_land,
                  Max(CASE
                        WHEN asmt_all.procname = 'CCAOFINAL' AND Cast(asmt_all.taxyr AS INT) < 2020 THEN valasm3
                        WHEN asmt_all.procname = 'CCAOFINAL' AND Cast(asmt_all.taxyr AS INT) = 2020 AND asmt_all.SEQ = 1 THEN valasm3
                        ELSE NULL
                      END) AS certified_tot,
                  Max(CASE
                        WHEN asmt_all.procname = 'BORVALUE' AND Cast(asmt_all.taxyr AS INT) < 2020 THEN userval5
                        ELSE NULL
                      END) AS board_bldg,
                  Max(CASE
                        WHEN asmt_all.procname = 'BORVALUE' AND Cast(asmt_all.taxyr AS INT) < 2020 THEN userval4
                        ELSE NULL
                      END) AS board_lanD,
                  Max(CASE
                        WHEN asmt_all.procname = 'BORVALUE' AND Cast(asmt_all.taxyr AS INT) < 2020 THEN valasm3
                        ELSE NULL
                      END) AS board_tot
           FROM   iasworld.asmt_all
                  left join iasworld.aprval
                         ON aprval.parid = asmt_all.parid
                            AND aprval.taxyr = asmt_all.taxyr
           -- Still working on 2020
           WHERE  Cast(asmt_all.taxyr AS INT) < Year(current_date) - 1
           GROUP  BY asmt_all.parid,
                     asmt_all.taxyr
           ORDER  BY asmt_all.parid,
                     asmt_all.taxyr)
  -- Add lagged values for previous two years
  SELECT parid                      AS pin,
         taxyr                      AS year,
         mailed_tot,
         certified_bldg,
         certified_land,
         certified_tot,
         board_bldg,
         board_land,
         board_tot,
         Lag(mailed_tot)
           over(
             PARTITION BY parid
             ORDER BY parid, taxyr) AS oneyr_pri_mailed_tot,
         Lag(certified_bldg)
           over(
             PARTITION BY parid
             ORDER BY parid, taxyr) AS oneyr_pri_certified_bldg,
         Lag(certified_land)
           over(
             PARTITION BY parid
             ORDER BY parid, taxyr) AS oneyr_pri_certified_land,
         Lag(certified_tot)
           over(
             PARTITION BY parid
             ORDER BY parid, taxyr) AS oneyr_pri_certified_tot,
         Lag(board_bldg)
           over(
             PARTITION BY parid
             ORDER BY parid, taxyr) AS oneyr_pri_board_bldg,
         Lag(board_land)
           over(
             PARTITION BY parid
             ORDER BY parid, taxyr) AS oneyr_pri_board_land,
         Lag(board_tot)
           over(
             PARTITION BY parid
             ORDER BY parid, taxyr) AS oneyr_pri_board_tot,
         Lag(mailed_tot, 2)
           over(
             PARTITION BY parid
             ORDER BY parid, taxyr) AS twoyr_pri_mailed_tot,
         Lag(certified_bldg, 2)
           over(
             PARTITION BY parid
             ORDER BY parid, taxyr) AS twoyr_pri_certified_bldg,
         Lag(certified_land, 2)
           over(
             PARTITION BY parid
             ORDER BY parid, taxyr) AS twoyr_pri_certified_land,
         Lag(certified_tot, 2)
           over(
             PARTITION BY parid
             ORDER BY parid, taxyr) AS twoyr_pri_certified_tot,
         Lag(board_bldg, 2)
           over(
             PARTITION BY parid
             ORDER BY parid, taxyr) AS twoyr_pri_board_bldg,
         Lag(board_land, 2)
           over(
             PARTITION BY parid
             ORDER BY parid, taxyr) AS twoyr_pri_board_land,
         Lag(board_tot, 2)
           over(
             PARTITION BY parid
             ORDER BY parid, taxyr) AS twoyr_pri_board_tot
  FROM   values_by_year