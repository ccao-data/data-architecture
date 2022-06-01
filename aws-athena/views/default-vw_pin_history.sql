 -- View containing current and prior years' assessments by PIN in wide format
CREATE OR replace VIEW default.vw_pin_history
AS
  -- CCAO mailed_tot, CCAO final, and BOR final values for each PIN by year
  WITH values_by_year
       AS (SELECT parid,
                  taxyr,
                  -- Mailed values
                  Max(CASE
                        WHEN procname = 'CCAOVALUE'
                             AND taxyr < '2020' THEN ovrvalasm2
                        WHEN procname = 'CCAOVALUE'
                             AND taxyr >= '2020'
                             AND valclass IS NULL THEN valasm2
                        ELSE NULL
                      END) AS mailed_bldg,
                  Max(CASE
                        WHEN procname = 'CCAOVALUE'
                             AND taxyr < '2020' THEN ovrvalasm1
                        WHEN procname = 'CCAOVALUE'
                             AND taxyr >= '2020'
                             AND valclass IS NULL THEN valasm1
                        ELSE NULL
                      END) AS mailed_land,
                  Max(CASE
                        WHEN procname = 'CCAOVALUE'
                             AND taxyr < '2020' THEN ovrvalasm3
                        WHEN procname = 'CCAOVALUE'
                             AND taxyr >= '2020'
                             AND valclass IS NULL THEN valasm3
                        ELSE NULL
                      END) AS mailed_tot,
                  -- Assessor certified values
                  Max(CASE
                        WHEN procname = 'CCAOFINAL'
                             AND taxyr < '2020' THEN ovrvalasm2
                        WHEN procname = 'CCAOFINAL'
                             AND taxyr >= '2020'
                             AND valclass IS NULL THEN valasm2
                        ELSE NULL
                      END) AS certified_bldg,
                  Max(CASE
                        WHEN procname = 'CCAOFINAL'
                             AND taxyr < '2020' THEN ovrvalasm1
                        WHEN procname = 'CCAOFINAL'
                             AND taxyr >= '2020'
                             AND valclass IS NULL THEN valasm1
                        ELSE NULL
                      END) AS certified_land,
                  Max(CASE
                        WHEN procname = 'CCAOFINAL'
                             AND taxyr < '2020' THEN ovrvalasm3
                        WHEN procname = 'CCAOFINAL'
                             AND taxyr >= '2020'
                             AND valclass IS NULL THEN valasm3
                        ELSE NULL
                      END) AS certified_tot,
                  -- Board certified values
                  Max(CASE
                        WHEN procname = 'BORVALUE'
                             AND taxyr < '2020' THEN ovrvalasm2
                        WHEN procname = 'BORVALUE'
                              AND valclass IS NULL
                             AND taxyr >= '2020' THEN valasm2
                        ELSE NULL
                      END) AS board_bldg,
                  Max(CASE
                        WHEN procname = 'BORVALUE'
                             AND taxyr < '2020' THEN ovrvalasm1
                         WHEN procname = 'BORVALUE'
                              AND valclass IS NULL
                             AND taxyr >= '2020' THEN valasm1
                        ELSE NULL
                      END) AS board_land,
                  Max(CASE
                        WHEN procname = 'BORVALUE'
                             AND taxyr < '2020' THEN ovrvalasm3
                             WHEN procname = 'BORVALUE'
                              AND valclass IS NULL
                             AND taxyr >= '2020' THEN valasm3
                        ELSE NULL
                      END) AS board_tot
           FROM   iasworld.asmt_all
           WHERE procname IS NOT NULL
           GROUP  BY parid,
                     taxyr
           ORDER  BY parid,
                     taxyr),
       -- Add township number and valuation class
       townships
       AS (SELECT parid,
                  taxyr,
                  class,
                  Substr(nbhd, 1, 2) AS township_code
           FROM   iasworld.pardat),
       -- Add township name
       town_names
       AS (SELECT township_name,
                  township_code
           FROM spatial.township)
  -- Add lagged values for previous two years
  SELECT values_by_year.parid                                     AS pin,
         values_by_year.taxyr                                     AS year,
         townships.class,
         townships.township_code,
         town_names.township_name,
         mailed_bldg,
         mailed_land,
         mailed_tot,
         certified_bldg,
         certified_land,
         certified_tot,
         board_bldg,
         board_land,
         board_tot,
         Lag(mailed_bldg)
           over(
             PARTITION BY values_by_year.parid
             ORDER BY values_by_year.parid, values_by_year.taxyr) AS
         oneyr_pri_mailed_bldg,
         Lag(mailed_land)
           over(
             PARTITION BY values_by_year.parid
             ORDER BY values_by_year.parid, values_by_year.taxyr) AS
         oneyr_pri_mailed_land,
         Lag(mailed_tot)
           over(
             PARTITION BY values_by_year.parid
             ORDER BY values_by_year.parid, values_by_year.taxyr) AS
         oneyr_pri_mailed_tot,
         Lag(certified_bldg)
           over(
             PARTITION BY values_by_year.parid
             ORDER BY values_by_year.parid, values_by_year.taxyr) AS
         oneyr_pri_certified_bldg,
         Lag(certified_land)
           over(
             PARTITION BY values_by_year.parid
             ORDER BY values_by_year.parid, values_by_year.taxyr) AS
         oneyr_pri_certified_land,
         Lag(certified_tot)
           over(
             PARTITION BY values_by_year.parid
             ORDER BY values_by_year.parid, values_by_year.taxyr) AS
         oneyr_pri_certified_tot,
         Lag(board_bldg)
           over(
             PARTITION BY values_by_year.parid
             ORDER BY values_by_year.parid, values_by_year.taxyr) AS
         oneyr_pri_board_bldg,
         Lag(board_land)
           over(
             PARTITION BY values_by_year.parid
             ORDER BY values_by_year.parid, values_by_year.taxyr) AS
         oneyr_pri_board_land,
         Lag(board_tot)
           over(
             PARTITION BY values_by_year.parid
             ORDER BY values_by_year.parid, values_by_year.taxyr) AS
         oneyr_pri_board_tot,
         Lag(mailed_tot, 2)
           over(
             PARTITION BY values_by_year.parid
             ORDER BY values_by_year.parid, values_by_year.taxyr) AS
         twoyr_pri_mailed_tot,
         Lag(certified_bldg, 2)
           over(
             PARTITION BY values_by_year.parid
             ORDER BY values_by_year.parid, values_by_year.taxyr) AS
         twoyr_pri_certified_bldg,
         Lag(certified_land, 2)
           over(
             PARTITION BY values_by_year.parid
             ORDER BY values_by_year.parid, values_by_year.taxyr) AS
         twoyr_pri_certified_land,
         Lag(certified_tot, 2)
           over(
             PARTITION BY values_by_year.parid
             ORDER BY values_by_year.parid, values_by_year.taxyr) AS
         twoyr_pri_certified_tot,
         Lag(board_bldg, 2)
           over(
             PARTITION BY values_by_year.parid
             ORDER BY values_by_year.parid, values_by_year.taxyr) AS
         twoyr_pri_board_bldg,
         Lag(board_land, 2)
           over(
             PARTITION BY values_by_year.parid
             ORDER BY values_by_year.parid, values_by_year.taxyr) AS
         twoyr_pri_board_land,
         Lag(board_tot, 2)
           over(
             PARTITION BY values_by_year.parid
             ORDER BY values_by_year.parid, values_by_year.taxyr) AS
         twoyr_pri_board_tot
  FROM   values_by_year
         left join townships
                ON values_by_year.parid = townships.parid
                   AND values_by_year.taxyr = townships.taxyr
         left join town_names
                ON townships.township_code = town_names.township_code