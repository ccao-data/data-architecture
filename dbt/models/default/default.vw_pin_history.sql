-- View containing current and prior years' assessments by PIN in wide format
SELECT
    vwpv.pin,
    vwpv.year,
    REGEXP_REPLACE(par.class, '[^[:alnum:]]', '') AS class,
    leg.user1 AS township_code,
    town.township_name,
    REGEXP_REPLACE(par.nbhd, '([^0-9])', '') AS nbhd,
    -- Add pre-mailed values to cover us for the period after the rollover but
    -- before the year's assessment cycle starts
    vwpv.pre_mailed_class,
    vwpv.pre_mailed_bldg,
    vwpv.pre_mailed_land,
    vwpv.pre_mailed_tot,
    vwpv.mailed_class,
    vwpv.mailed_bldg,
    vwpv.mailed_land,
    vwpv.mailed_tot,
    vwpv.certified_class,
    vwpv.certified_bldg,
    vwpv.certified_land,
    vwpv.certified_tot,
    vwpv.board_class,
    vwpv.board_bldg,
    vwpv.board_land,
    vwpv.board_tot,
    vwpv.change_reason,
    -- Add lagged values for previous two years
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
    LAG(vwpv.change_reason) OVER (
        PARTITION BY vwpv.pin
        ORDER BY vwpv.pin, vwpv.year
    ) AS oneyr_pri_change_reason,
    LAG(vwpv.mailed_bldg, 2) OVER (
        PARTITION BY vwpv.pin
        ORDER BY vwpv.pin, vwpv.year
    ) AS twoyr_pri_mailed_bldg,
    LAG(vwpv.mailed_land, 2) OVER (
        PARTITION BY vwpv.pin
        ORDER BY vwpv.pin, vwpv.year
    ) AS twoyr_pri_mailed_land,
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
    ) AS twoyr_pri_board_tot,
    LAG(vwpv.change_reason, 2) OVER (
        PARTITION BY vwpv.pin
        ORDER BY vwpv.pin, vwpv.year
    ) AS twoyr_pri_change_reason

FROM {{ ref('default.vw_pin_value') }} AS vwpv
LEFT JOIN {{ source('iasworld', 'legdat') }} AS leg
    ON vwpv.pin = leg.parid
    AND vwpv.year = leg.taxyr
    AND leg.cur = 'Y'
    AND leg.deactivat IS NULL
INNER JOIN {{ source('iasworld', 'pardat') }} AS par
    ON vwpv.pin = par.parid
    AND vwpv.year = par.taxyr
    AND par.cur = 'Y'
    AND par.deactivat IS NULL
LEFT JOIN {{ source('spatial', 'township') }} AS town
    ON leg.user1 = town.township_code
