-- View containing current and prior years' assessments by PIN in wide format
CREATE OR REPLACE VIEW default.vw_pin_history AS
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
    vwpv.pin,
    vwpv.year,
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
    LAG(mailed_bldg) OVER (
        PARTITION BY vwpv.pin
        ORDER BY vwpv.pin, vwpv.year
    ) AS oneyr_pri_mailed_bldg,
    LAG(mailed_land) OVER (
        PARTITION BY vwpv.pin
        ORDER BY vwpv.pin, vwpv.year
    ) AS oneyr_pri_mailed_land,
    LAG(mailed_tot) OVER (
        PARTITION BY vwpv.pin
        ORDER BY vwpv.pin, vwpv.year
    ) AS oneyr_pri_mailed_tot,
    LAG(certified_bldg) OVER (
        PARTITION BY vwpv.pin
        ORDER BY vwpv.pin, vwpv.year
    ) AS oneyr_pri_certified_bldg,
    LAG(certified_land) OVER (
        PARTITION BY vwpv.pin
        ORDER BY vwpv.pin, vwpv.year
    ) AS oneyr_pri_certified_land,
    LAG(certified_tot) OVER (
        PARTITION BY vwpv.pin
        ORDER BY vwpv.pin, vwpv.year
    ) AS oneyr_pri_certified_tot,
    LAG(board_bldg) OVER (
        PARTITION BY vwpv.pin
        ORDER BY vwpv.pin, vwpv.year
    ) AS oneyr_pri_board_bldg,
    LAG(board_land) OVER (
        PARTITION BY vwpv.pin
        ORDER BY vwpv.pin, vwpv.year
    ) AS oneyr_pri_board_land,
    LAG(board_tot) OVER (
        PARTITION BY vwpv.pin
        ORDER BY vwpv.pin, vwpv.year
    ) AS oneyr_pri_board_tot,
    LAG(mailed_tot, 2) OVER (
        PARTITION BY vwpv.pin
        ORDER BY vwpv.pin, vwpv.year
    ) AS twoyr_pri_mailed_tot,
    LAG(certified_bldg, 2) OVER (
        PARTITION BY vwpv.pin
        ORDER BY vwpv.pin, vwpv.year
    ) AS twoyr_pri_certified_bldg,
    LAG(certified_land, 2) OVER (
        PARTITION BY vwpv.pin
        ORDER BY vwpv.pin, vwpv.year
    ) AS twoyr_pri_certified_land,
    LAG(certified_tot, 2) OVER (
        PARTITION BY vwpv.pin
        ORDER BY vwpv.pin, vwpv.year
    ) AS twoyr_pri_certified_tot,
    LAG(board_bldg, 2) OVER (
        PARTITION BY vwpv.pin
        ORDER BY vwpv.pin, vwpv.year
    ) AS twoyr_pri_board_bldg,
    LAG(board_land, 2) OVER (
        PARTITION BY vwpv.pin
        ORDER BY vwpv.pin, vwpv.year
    ) AS twoyr_pri_board_land,
    LAG(board_tot, 2) OVER (
        PARTITION BY vwpv.pin
        ORDER BY vwpv.pin, vwpv.year
    ) AS twoyr_pri_board_tot

FROM default.vw_pin_value AS vwpv
LEFT JOIN townships
    ON vwpv.pin = townships.parid
    AND vwpv.year = townships.taxyr
LEFT JOIN classes
    ON vwpv.pin = classes.parid
    AND vwpv.year = classes.taxyr
LEFT JOIN town_names
    ON townships.township_code = town_names.township_code
