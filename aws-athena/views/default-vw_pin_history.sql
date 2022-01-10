 -- View containing current and prior years' assessments by PIN in wide format
CREATE OR replace VIEW default.vw_pin_history
AS
  -- CCAO mailed, CCAO final, and BOR final values for each PIN by year
  WITH values_by_year
       AS (SELECT parid,
                  taxyr,
                  Max(CASE
                        WHEN procname = 'CCAOVALUE' THEN valasm3
                        ELSE NULL
                      END) AS ccao_mailed,
                  Max(CASE
                        WHEN procname = 'CCAOFINAL' THEN valasm3
                        ELSE NULL
                      END) AS ccao_final,
                  Max(CASE
                        WHEN procname = 'BORVALUE' THEN valasm3
                        ELSE NULL
                      END) AS bor_final
           FROM   iasworld.asmt_all
           -- Still working on 2020
           WHERE  Cast(taxyr AS INT) < Year(current_date) - 2
           GROUP  BY parid,
                     taxyr
           ORDER  BY parid,
                     taxyr)
  -- Add lagged values for previous two years
  SELECT parid                      AS pin,
         taxyr                      AS year,
         ccao_mailed,
         ccao_final,
         bor_final,
         Lag(ccao_mailed)
           over(
             PARTITION BY parid
             ORDER BY parid, taxyr) AS pri1_ccao_mailed,
         Lag(ccao_final)
           over(
             PARTITION BY parid
             ORDER BY parid, taxyr) AS pri1_ccao_final,
         Lag(bor_final)
           over(
             PARTITION BY parid
             ORDER BY parid, taxyr) AS pri1_bor_final,
         Lag(ccao_mailed, 2)
           over(
             PARTITION BY parid
             ORDER BY parid, taxyr) AS pri2_ccao_mailed,
         Lag(ccao_final, 2)
           over(
             PARTITION BY parid
             ORDER BY parid, taxyr) AS pri2_ccao_final,
         Lag(bor_final, 2)
           over(
             PARTITION BY parid
             ORDER BY parid, taxyr) AS pri2_bor_final
  FROM   values_by_year