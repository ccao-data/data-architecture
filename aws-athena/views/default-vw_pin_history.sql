-- View containing current and prior years' assessments by PIN in wide format
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
    vwpv.mailed_bldg,
    vwpv.mailed_land,
    vwpv.mailed_tot,
    vwpv.certified_bldg,
    vwpv.certified_land,
    vwpv.certified_tot,
    vwpv.board_bldg,
    vwpv.board_land,
    vwpv.board_tot,
    LAG(vwpv.mailed_bldg) OVER (
        PARTITION BY vwpv.pin
        ORDER BY vwpv.pin, vwpv.year
    ) AS oneyr_pri_mailed_bldg,
    LAG(vwpv.mailed_land) OVER (
        PARTITION BY vwpv.pin
        ORDER BY vwpv.pin, vwpv.year
    ) AS oneyr_pri_mailed_land,
    LAG(vwpv.mailed_tot) OVER (
        PARTITION BY vwpv.pin
        ORDER BY vwpv.pin, vwpv.year
    ) AS oneyr_pri_mailed_tot,
    LAG(vwpv.certified_bldg) OVER (
        PARTITION BY vwpv.pin
        ORDER BY vwpv.pin, vwpv.year
    ) AS oneyr_pri_certified_bldg,
    LAG(vwpv.certified_land) OVER (
        PARTITION BY vwpv.pin
        ORDER BY vwpv.pin, vwpv.year
    ) AS oneyr_pri_certified_land,
    LAG(vwpv.certified_tot) OVER (
        PARTITION BY vwpv.pin
        ORDER BY vwpv.pin, vwpv.year
    ) AS oneyr_pri_certified_tot,
    LAG(vwpv.board_bldg) OVER (
        PARTITION BY vwpv.pin
        ORDER BY vwpv.pin, vwpv.year
    ) AS oneyr_pri_board_bldg,
    LAG(vwpv.board_land) OVER (
        PARTITION BY vwpv.pin
        ORDER BY vwpv.pin, vwpv.year
    ) AS oneyr_pri_board_land,
    LAG(vwpv.board_tot) OVER (
        PARTITION BY vwpv.pin
        ORDER BY vwpv.pin, vwpv.year
    ) AS oneyr_pri_board_tot,
    LAG(vwpv.mailed_tot, 2) OVER (
        PARTITION BY vwpv.pin
        ORDER BY vwpv.pin, vwpv.year
    ) AS twoyr_pri_mailed_tot,
    LAG(vwpv.certified_bldg, 2) OVER (
        PARTITION BY vwpv.pin
        ORDER BY vwpv.pin, vwpv.year
    ) AS twoyr_pri_certified_bldg,
    LAG(vwpv.certified_land, 2) OVER (
        PARTITION BY vwpv.pin
        ORDER BY vwpv.pin, vwpv.year
    ) AS twoyr_pri_certified_land,
    LAG(vwpv.certified_tot, 2) OVER (
        PARTITION BY vwpv.pin
        ORDER BY vwpv.pin, vwpv.year
    ) AS twoyr_pri_certified_tot,
    LAG(vwpv.board_bldg, 2) OVER (
        PARTITION BY vwpv.pin
        ORDER BY vwpv.pin, vwpv.year
    ) AS twoyr_pri_board_bldg,
    LAG(vwpv.board_land, 2) OVER (
        PARTITION BY vwpv.pin
        ORDER BY vwpv.pin, vwpv.year
    ) AS twoyr_pri_board_land,
    LAG(vwpv.board_tot, 2) OVER (
        PARTITION BY vwpv.pin
        ORDER BY vwpv.pin, vwpv.year
    ) AS twoyr_pri_board_tot

FROM {{ ref('vw_pin_value_test') }} AS vwpv
LEFT JOIN townships
    ON vwpv.pin = townships.parid
    AND vwpv.year = townships.taxyr
LEFT JOIN classes
    ON vwpv.pin = classes.parid
    AND vwpv.year = classes.taxyr
LEFT JOIN town_names
    ON townships.township_code = town_names.township_code
