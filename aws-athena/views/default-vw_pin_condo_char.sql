/**
View containing cleaned, filled data for condo modeling. Missing data is
filled with the following steps:

All historical data is filled FORWARD in time, i.e. data from 2020 fills
2021 as long as the data isn't something which frequently changes
**/

CREATE OR replace VIEW default.vw_pin_condo_char
AS
WITH aggregate_land AS (
    SELECT
        parid,
        taxyr,
        CASE WHEN Count(*) > 1 THEN TRUE
            ELSE FALSE
            END AS pin_is_multiland,
            Count(*) AS pin_num_landlines,
            SUM(sf) AS total_building_land_sf
    FROM iasworld.land
    GROUP BY parid,taxyr
    ),
-- Generate our own count of building units by year
unique_pins AS (
    SELECT DISTINCT
        parid,
        Substr(parid, 1, 10) AS pin10,
        taxyr as year
    FROM iasworld.pardat
    WHERE class in ('299', '399')
    ),
units AS (
    SELECT
        pin10,
        year,
        Count(*) AS building_units
    FROM unique_pins
    GROUP BY pin10, year
    ),
-- All characteristics associated with condos in the OBY/COMDAT tables
all_chars AS (
    (SELECT DISTINCT
        parid AS pin,
        Substr(parid, 1, 10) AS pin10,
        class,
        seq,
        cur,
        taxyr AS year,
        user16 AS cdu,
        -- Very rarely use 'effyr' rather than 'yrblt' when 'yrblt' is NULL
        CASE WHEN yrblt IS NULL THEN effyr
            ELSE yrblt
            END AS char_yrblt,
        cond AS char_cond,
        grade AS char_grade
    FROM iasworld.oby
    WHERE class IN ( '299')
        AND cur = 'Y')
    UNION
    (SELECT DISTINCT
        parid AS pin,
        Substr(parid, 1, 10) AS pin10,
        class,
        seq,
        cur,
        taxyr AS year,
        user16 AS cdu,
        -- Very rarely use 'effyr' rather than 'yrblt' when 'yrblt' is NULL
        CASE WHEN yrblt IS NULL THEN effyr
            ELSE yrblt
            END AS char_yrblt,
        cdu AS char_cond,
        grade AS char_grade
    FROM iasworld.comdat
    WHERE class IN ( '399')
        AND cur = 'Y')
        ),
-- Have to INNER JOIN by class with pardat since some PINs show up in both OBY and COMDAT but with differnt classes...
chars AS (
    SELECT * FROM (
        SELECT
            all_chars.*,
            max(char_yrblt)
                OVER (PARTITION BY all_chars.pin, all_chars.year)
                AS max_yrblt
        FROM all_chars
        INNER JOIN (
            SELECT
                parid AS pin,
                taxyr AS year,
                class FROM iasworld.pardat
            ) pardat
        ON all_chars.pin = pardat.pin
            AND all_chars.year = pardat.year
            AND all_chars.class = pardat.class
    )
    WHERE char_yrblt = max_yrblt
),
-- Unit numbers and notes, used to help fing parking spaces
unit_numbers AS (
    SELECT DISTINCT
        parid AS pin,
        taxyr AS year,
        unitdesc,
        unitno,
        tiebldgpct,
        note2 as note
    FROM iasworld.pardat
    WHERE unitdesc IS NOT NULL
        OR unitno IS NOT NULL
        ),
-- CDUs are not well-maintained year-to-year, we'll forward fill NULLs to account for this
forward_fill AS (
    SELECT
        chars.pin,
        chars.year,
        CASE
            WHEN cdu IS NULL
            THEN LAST_VALUE(chars.cdu) IGNORE NULLS
                OVER (PARTITION BY chars.pin ORDER BY chars.year)
            ELSE chars.cdu
            END AS cdu,
        CASE
            WHEN note IS NULL
            THEN LAST_VALUE(unit_numbers.note) IGNORE NULLS
                OVER (PARTITION BY chars.pin ORDER BY chars.year)
            ELSE unit_numbers.note
            END AS note
    FROM chars
    LEFT JOIN unit_numbers ON chars.pin = unit_numbers.pin AND chars.year = unit_numbers.year
),
-- Prior year AV, used to help fing parking spaces and common areas
prior_values AS (
    SELECT
        pin,
        year,
        oneyr_pri_board_tot
    FROM default.vw_pin_history
)

SELECT
    chars.pin,
    chars.pin10,
    chars.year,
    chars.class,
    chars.seq,
    chars.cur,
    chars.char_yrblt,
    CASE WHEN chars.char_cond IN ('AV', 'A') THEN 'Average'
        WHEN chars.char_cond IN ('GD', 'G') THEN 'Good'
        WHEN chars.char_cond = 'V' THEN 'Very Good'
        WHEN chars.char_cond = 'F' THEN 'Fair'
        ELSE NULL END AS char_cond,
    CASE WHEN chars.char_grade IN ('2', 'A') THEN 'Average'
        WHEN chars.char_grade = 'C' THEN 'Good'
        ELSE NULL END AS char_grade,
    units.building_units as char_building_units,
    unit_numbers.tiebldgpct as char_tiebldgpct,
    total_building_land_sf as char_land_sf,

    forward_fill.cdu,
    forward_fill.note,
    unit_numbers.unitno,
    prior_values.oneyr_pri_board_tot,
    CASE
        WHEN forward_fill.cdu = 'GR'
            OR SUBSTR(unit_numbers.unitno, 1, 1) = 'P'
            OR SUBSTR(unit_numbers.unitno, 1, 3) = 'GAR'
            -- If a unit's percent of the declaration is less than half of what it would be if all units had an equal share, AV limited
            OR (unit_numbers.tiebldgpct < (50 / units.building_units) AND prior_values.oneyr_pri_board_tot BETWEEN 10 AND 5000)
            OR prior_values.oneyr_pri_board_tot BETWEEN 10 AND 1000
        THEN TRUE
        ELSE FALSE
    END AS is_parking_space,
    CASE
        WHEN forward_fill.cdu = 'GR' then 'cdu'
        WHEN SUBSTR(unit_numbers.unitno, 1, 1) = 'P'
            OR SUBSTR(unit_numbers.unitno, 1, 3) = 'GAR' THEN 'unit number'
        WHEN (unit_numbers.tiebldgpct < (50 / units.building_units) AND prior_values.oneyr_pri_board_tot BETWEEN 10 AND 5000) THEN 'declaration percent'
        WHEN prior_values.oneyr_pri_board_tot BETWEEN 10 AND 1000 THEN 'prior value'
        ELSE NULL
    END AS parking_space_flag_reason,

    CASE
        WHEN prior_values.oneyr_pri_board_tot < 10
        THEN TRUE
        ELSE FALSE
    END AS is_common_area,

    pin_is_multiland,
    pin_num_landlines
FROM chars
LEFT JOIN aggregate_land
ON chars.pin = aggregate_land.parid
    AND chars.year = aggregate_land.taxyr
LEFT JOIN units
ON chars.pin10 = units.pin10
    AND chars.year = units.year
LEFT JOIN unit_numbers
ON chars.pin = unit_numbers.pin
    AND chars.year = unit_numbers.year
LEFT JOIN forward_fill
ON chars.pin = forward_fill.pin
    AND chars.year = forward_fill.year
LEFT JOIN prior_values
ON chars.pin = prior_values.pin
    AND chars.year = prior_values.year