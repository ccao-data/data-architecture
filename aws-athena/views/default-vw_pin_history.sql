 -- View containing current and prior years' assessments by PIN in wide format
CREATE OR replace VIEW default.vw_pin_history
AS
  -- Add valuation class
  WITH classes AS (
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
      user1 AS township_code
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
  vwpv.parid AS pin,
  vwpv.taxyr AS year,
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
    PARTITION BY vwpv.parid
    ORDER BY vwpv.parid, vwpv.taxyr
    ) AS oneyr_pri_mailed_bldg,
  Lag(mailed_land) over(
    PARTITION BY vwpv.parid
    ORDER BY vwpv.parid, vwpv.taxyr
    ) AS oneyr_pri_mailed_land,
  Lag(mailed_tot) over(
    PARTITION BY vwpv.parid
    ORDER BY vwpv.parid, vwpv.taxyr
    ) AS oneyr_pri_mailed_tot,
  Lag(certified_bldg) over(
    PARTITION BY vwpv.parid
    ORDER BY vwpv.parid, vwpv.taxyr
    ) AS oneyr_pri_certified_bldg,
  Lag(certified_land) over(
    PARTITION BY vwpv.parid
    ORDER BY vwpv.parid, vwpv.taxyr
    ) AS oneyr_pri_certified_land,
  Lag(certified_tot) over(
    PARTITION BY vwpv.parid
    ORDER BY vwpv.parid, vwpv.taxyr
    ) AS oneyr_pri_certified_tot,
  Lag(board_bldg) over(
    PARTITION BY vwpv.parid
    ORDER BY vwpv.parid, vwpv.taxyr
    ) AS oneyr_pri_board_bldg,
  Lag(board_land) over(
    PARTITION BY vwpv.parid
    ORDER BY vwpv.parid, vwpv.taxyr
    ) AS oneyr_pri_board_land,
  Lag(board_tot) over(
    PARTITION BY vwpv.parid
    ORDER BY vwpv.parid, vwpv.taxyr
    ) AS oneyr_pri_board_tot,
  Lag(mailed_tot, 2) over(
    PARTITION BY vwpv.parid
    ORDER BY vwpv.parid, vwpv.taxyr
    ) AS twoyr_pri_mailed_tot,
  Lag(certified_bldg, 2) over(
    PARTITION BY vwpv.parid
    ORDER BY vwpv.parid, vwpv.taxyr
    ) AS twoyr_pri_certified_bldg,
  Lag(certified_land, 2) over(
    PARTITION BY vwpv.parid
    ORDER BY vwpv.parid, vwpv.taxyr
    ) AS twoyr_pri_certified_land,
  Lag(certified_tot, 2) over(
    PARTITION BY vwpv.parid
    ORDER BY vwpv.parid, vwpv.taxyr
    ) AS twoyr_pri_certified_tot,
  Lag(board_bldg, 2) over(
    PARTITION BY vwpv.parid
    ORDER BY vwpv.parid, vwpv.taxyr
    ) AS twoyr_pri_board_bldg,
  Lag(board_land, 2) over(
    PARTITION BY vwpv.parid
    ORDER BY vwpv.parid, vwpv.taxyr
    ) AS twoyr_pri_board_land,
  Lag(board_tot, 2) over(
    PARTITION BY vwpv.parid
    ORDER BY vwpv.parid, vwpv.taxyr
    ) AS twoyr_pri_board_tot

FROM default.vw_pin_value vwpv
LEFT JOIN townships ON vwpv.parid = townships.parid
  AND vwpv.taxyr = townships.taxyr
LEFT JOIN classes ON vwpv.parid = classes.parid
  AND vwpv.taxyr = classes.taxyr
LEFT JOIN town_names ON townships.township_code = town_names.township_code
