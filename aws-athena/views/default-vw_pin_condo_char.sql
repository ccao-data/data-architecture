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
    WHERE class in ('299', '399')),
units AS (
    SELECT
        pin10,
        year,
        Count(*) AS building_units
    FROM unique_pins
    GROUP BY pin10, year
    ),
-- All characteristics associated with condos in the OBY table
chars AS (
    SELECT DISTINCT
        parid AS pin,
        Substr(parid, 1, 10) AS pin10,
        who AS updated_by,
        Date_parse(wen, '%Y-%m-%d %H:%i:%s.%f') AS updated_at,
        CASE WHEN wen = (
            Max(wen)
                over(PARTITION BY parid, taxyr, cur )
            ) THEN TRUE
            ELSE FALSE
            END AS is_most_recent_update,
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
    WHERE class IN ( '299', '399' )
        ),
-- Unit numbers, used to help fing parking spaces
unit_numbers AS (
    SELECT DISTINCT
        parid AS pin,taxyr AS year,
        unitdesc,
        unitno,
        tiebldgpct
    FROM iasworld.pardat
    WHERE unitdesc IS NOT NULL
        OR unitno IS NOT NULL
        ),
-- CDUs are not well-maintained year-to-year, we'll forward fill NULLs to account for this
forward_fill AS (
    SELECT
        pin,
        year,
        CASE
            WHEN cdu IS NULL
            THEN LAST_VALUE(chars.cdu) IGNORE NULLS
                OVER (PARTITION BY chars.pin ORDER BY chars.year)
            ELSE chars.cdu
            END AS cdu
    FROM chars
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
    chars.updated_by,
    chars.updated_at,
    chars.is_most_recent_update,
    chars.seq,
    chars.cur,
    chars.char_yrblt,
    chars.char_cond,
    chars.char_grade,

    forward_fill.cdu,
    units.building_units,
    unit_numbers.tiebldgpct,
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
    pin_num_landlines,
    total_building_land_sf
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