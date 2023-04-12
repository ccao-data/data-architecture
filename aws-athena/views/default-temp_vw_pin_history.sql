 -- View containing current and prior years' assessments by PIN in wide format
CREATE OR replace VIEW default.temp_vw_pin_history
AS
  -- Add valuation class
  WITH
  classes AS (
    SELECT
      parid,
      taxyr,
      class
    FROM iasworld.pardat
    ),
  -- Add township number
  townships AS (
    SELECT
      parid,
      taxyr,
      substr(TAXDIST, 1, 2) AS township_code
  FROM iasworld.legdat
  ),
  -- Add township name
  town_names AS (
    SELECT
      township_name,
      township_code
    FROM spatial.township
    )
  -- Add lagged values for previous two years
SELECT
  tvpv.parid AS pin,
  tvpv.taxyr AS year,
  classes.class,
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
  Lag(mailed_bldg) over(
    PARTITION BY tvpv.parid
    ORDER BY tvpv.parid, tvpv.taxyr
    ) AS oneyr_pri_mailed_bldg,
  Lag(mailed_land) over(
    PARTITION BY tvpv.parid
    ORDER BY tvpv.parid, tvpv.taxyr
    ) AS oneyr_pri_mailed_land,
  Lag(mailed_tot) over(
    PARTITION BY tvpv.parid
    ORDER BY tvpv.parid, tvpv.taxyr
    ) AS oneyr_pri_mailed_tot,
  Lag(certified_bldg) over(
    PARTITION BY tvpv.parid
    ORDER BY tvpv.parid, tvpv.taxyr
    ) AS oneyr_pri_certified_bldg,
  Lag(certified_land) over(
    PARTITION BY tvpv.parid
    ORDER BY tvpv.parid, tvpv.taxyr
    ) AS oneyr_pri_certified_land,
  Lag(certified_tot) over(
    PARTITION BY tvpv.parid
    ORDER BY tvpv.parid, tvpv.taxyr
    ) AS oneyr_pri_certified_tot,
  Lag(board_bldg) over(
    PARTITION BY tvpv.parid
    ORDER BY tvpv.parid, tvpv.taxyr
    ) AS oneyr_pri_board_bldg,
  Lag(board_land) over(
    PARTITION BY tvpv.parid
    ORDER BY tvpv.parid, tvpv.taxyr
    ) AS oneyr_pri_board_land,
  Lag(board_tot) over(
    PARTITION BY tvpv.parid
    ORDER BY tvpv.parid, tvpv.taxyr
    ) AS oneyr_pri_board_tot,
  Lag(mailed_tot, 2) over(
    PARTITION BY tvpv.parid
    ORDER BY tvpv.parid, tvpv.taxyr
    ) AS twoyr_pri_mailed_tot,
  Lag(certified_bldg, 2) over(
    PARTITION BY tvpv.parid
    ORDER BY tvpv.parid, tvpv.taxyr
    ) AS twoyr_pri_certified_bldg,
  Lag(certified_land, 2) over(
    PARTITION BY tvpv.parid
    ORDER BY tvpv.parid, tvpv.taxyr
    ) AS twoyr_pri_certified_land,
  Lag(certified_tot, 2) over(
    PARTITION BY tvpv.parid
    ORDER BY tvpv.parid, tvpv.taxyr
    ) AS twoyr_pri_certified_tot,
  Lag(board_bldg, 2) over(
    PARTITION BY tvpv.parid
    ORDER BY tvpv.parid, tvpv.taxyr
    ) AS twoyr_pri_board_bldg,
  Lag(board_land, 2) over(
    PARTITION BY tvpv.parid
    ORDER BY tvpv.parid, tvpv.taxyr
    ) AS twoyr_pri_board_land,
  Lag(board_tot, 2) over(
    PARTITION BY tvpv.parid
    ORDER BY tvpv.parid, tvpv.taxyr
    ) AS twoyr_pri_board_tot

FROM default.temp_vw_pin_value tvpv
LEFT JOIN townships ON tvpv.parid = townships.parid
  AND tvpv.taxyr = townships.taxyr
LEFT JOIN classes ON tvpv.parid = classes.parid
  AND tvpv.taxyr = classes.taxyr
LEFT JOIN town_names ON townships.township_code = town_names.township_code