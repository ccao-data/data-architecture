/*
View containing cleaned, filled data for condo modeling. Missing data is
filled as follows:

Condo characteristics are filled with whatever the most recent non-NULL
value is. This assumes that new condo data is more accurate than older
data, not that it represents a change in a unit's characteristics. This
should only be the case while condo characteristics are pulled from excel
workbooks rather than iasWorld.
*/

WITH aggregate_land AS (
    SELECT
        pin AS parid,
        year AS taxyr,
        COALESCE(num_landlines > 1, FALSE) AS pin_is_multiland,
        num_landlines AS pin_num_landlines,
        sf AS total_building_land_sf
    FROM {{ ref('default.vw_pin_land') }}
),

-- These two filtered queries exist only to make sure condos pulled from
-- OBY and COMDAT are unique by pin and taxyr
oby_filtered AS (
    SELECT
        *,
        ROW_NUMBER()
            OVER (PARTITION BY parid, taxyr ORDER BY lline ASC)
            AS row_no,
        COUNT()
            OVER (PARTITION BY parid, taxyr)
            AS num_lines
    FROM {{ source('iasworld', 'oby') }}
    -- We don't include DEACTIVAT IS NULL here since it can disagree with
    -- DEACTIVAT in iasworld.pardat and we'll defer to that table
    WHERE cur = 'Y'
        AND class IN ('299', '399')
),

comdat_filtered AS (
    SELECT
        *,
        ROW_NUMBER()
            OVER (PARTITION BY parid, taxyr ORDER BY card ASC)
            AS row_no,
        COUNT()
            OVER (PARTITION BY parid, taxyr)
            AS num_lines
    FROM {{ source('iasworld', 'comdat') }}
    -- We don't include DEACTIVAT IS NULL here since it can disagree with
    -- DEACTIVAT in iasworld.pardat and we'll defer to that table
    WHERE cur = 'Y'
        AND class IN ('299', '399')
),

-- All characteristics associated with condos in
-- the OBY (299s) / COMDAT (399s) tables
chars AS (
    -- Distinct because oby and comdat contain multiple cards for a few condos
    SELECT
        par.parid AS pin,
        CASE
            WHEN
                (oby.card IS NULL OR com.card IS NULL)
                THEN COALESCE(oby.card, com.card)
            WHEN
                REGEXP_REPLACE(par.class, '[^[:alnum:]]', '') = '299'
                THEN oby.card
            WHEN par.class = '399' THEN com.card
        END AS card,
        -- Proration related fields from PARDAT
        par.tieback AS tieback_key_pin,
        CASE
            WHEN
                par.tiebldgpct IS NOT NULL
                THEN par.tiebldgpct / 100.0
            WHEN
                par.tiebldgpct IS NULL
                THEN 0
            ELSE 1.0
        END AS tieback_proration_rate,
        CASE
            WHEN (oby.user20 IS NULL OR com.user24 IS NULL)
                THEN CAST(COALESCE(oby.user20, com.user24) AS DOUBLE) / 100.0
            WHEN
                REGEXP_REPLACE(par.class, '[^[:alnum:]]', '') = '299'
                THEN CAST(oby.user20 AS DOUBLE) / 100.0
            WHEN
                par.class = '399'
                THEN CAST(com.user24 AS DOUBLE) / 100.0
        END AS card_proration_rate,
        oby.lline,
        COALESCE(oby.num_lines, com.num_lines) AS num_lines,
        SUBSTR(par.parid, 1, 10) AS pin10,
        REGEXP_REPLACE(par.class, '[^[:alnum:]]', '') AS class,
        par.taxyr AS year,
        leg.user1 AS township_code,
        CASE
            WHEN
                REGEXP_REPLACE(par.class, '[^[:alnum:]]', '') = '299'
                THEN oby.user16
            WHEN
                par.class = '399' AND nonlivable.flag != '399 GR'
                THEN com.user16
            WHEN
                par.class = '399' AND nonlivable.flag = '399 GR'
                THEN 'GR'
        END AS cdu,
        -- Very rarely use 'effyr' rather than 'yrblt' when 'yrblt' is NULL
        CASE
            WHEN
                REGEXP_REPLACE(par.class, '[^[:alnum:]]', '') = '299'
                THEN COALESCE(
                    oby.yrblt, oby.effyr, com.yrblt, com.effyr
                )
            WHEN
                par.class = '399'
                THEN COALESCE(
                    com.yrblt, com.effyr, oby.yrblt, oby.effyr
                )
        END AS char_yrblt,
        MAX(
            CASE
                WHEN
                    REGEXP_REPLACE(par.class, '[^[:alnum:]]', '') = '299'
                    THEN COALESCE(
                        oby.yrblt, oby.effyr, com.yrblt, com.effyr
                    )
                WHEN
                    par.class = '399'
                    THEN COALESCE(
                        com.yrblt, com.effyr, oby.yrblt, oby.effyr
                    )
            END
        )
            OVER (PARTITION BY par.parid, par.taxyr)
            AS max_yrblt,
        CAST(ROUND(pin_condo_char.building_sf, 0) AS INT)
            AS char_building_sf,
        CAST(ROUND(pin_condo_char.unit_sf, 0) AS INT) AS char_unit_sf,
        CAST(pin_condo_char.bedrooms AS INT) AS char_bedrooms,
        CAST(pin_condo_char.half_baths AS INT) AS char_half_baths,
        CAST(pin_condo_char.full_baths AS INT) AS char_full_baths,
        pin_condo_char.parking_pin,
        par.unitno,
        par.tiebldgpct,
        par.note2 AS note,
        COALESCE(SUM(
            CASE
                WHEN
                    REGEXP_REPLACE(par.class, '[^[:alnum:]]', '') NOT IN (
                        '299', '399'
                    )
                    THEN 1
                ELSE 0
            END
        )
            OVER (
                PARTITION BY SUBSTR(par.parid, 1, 10), par.taxyr
            )
        > 0, FALSE)
            AS bldg_is_mixed_use
    FROM {{ source('iasworld', 'pardat') }} AS par

    -- Left joins because par contains both 299s & 399s (oby and comdat
    -- do not) and pin_condo_char doesn't contain all condos
    LEFT JOIN oby_filtered AS oby
        ON par.parid = oby.parid
        AND par.taxyr = oby.taxyr
    LEFT JOIN comdat_filtered AS com
        ON par.parid = com.parid
        AND par.taxyr = com.taxyr
    LEFT JOIN {{ source('ccao', 'pin_condo_char') }} AS pin_condo_char
        ON par.parid = pin_condo_char.pin
    LEFT JOIN {{ source('iasworld', 'legdat') }} AS leg
        ON par.parid = leg.parid
        AND par.taxyr = leg.taxyr
        --
        /* 1) Valuations-provided PINs that shouldn't be considered parking
        spaces 2) In the process of QC'ing condo data, we discovered that some
        condo units received (unused) negative predicted values. These units
        were non-livable units incorrectly classified as livable. They received
        negative predicted values due to their very low % of ownership. This CTE
        excludes them from the model going forward. 3) Questionable garage units
        are those that have been deemed nonlivable by some part of our
        nonlivable detection, but upon human review have been deemed livable. */
    LEFT JOIN {{ source('ccao', 'pin_nonlivable') }} AS nonlivable
        ON par.parid = nonlivable.pin
    WHERE par.cur = 'Y'
        AND par.deactivat IS NULL
        AND (oby.row_no = 1 OR com.row_no = 1)
),

filled AS (
    -- Backfilling data since it's rarely updated
    SELECT
        pin,
        pin10,
        card,
        lline,
        num_lines,
        class,
        year,
        township_code,
        char_yrblt,
        -- CDUs/notes are not well-maintained year-to-year
        -- but CDU CAN be updated to NULL
        FIRST_VALUE(cdu)
            OVER (PARTITION BY pin ORDER BY year DESC)
            AS cdu,
        COALESCE(note, LAST_VALUE(
            note) IGNORE NULLS
        OVER (PARTITION BY pin ORDER BY year DESC)) AS note,
        -- Characteristics data gathered from MLS by valuations
        char_building_sf,
        char_unit_sf,
        char_bedrooms,
        char_half_baths,
        char_full_baths,
        parking_pin,
        unitno,
        tiebldgpct,
        tieback_key_pin,
        tieback_proration_rate,
        card_proration_rate,
        bldg_is_mixed_use,
        COUNT(*)
            OVER (PARTITION BY pin10, year)
            AS building_pins
    FROM chars
    WHERE class IN ('299', '399')
)

SELECT DISTINCT
    filled.pin,
    filled.pin10,
    filled.card,
    filled.lline,
    filled.year,
    filled.class,
    filled.township_code,
    filled.num_lines > 1 AS pin_is_multilline,
    filled.num_lines AS pin_num_lline,
    filled.tieback_key_pin,
    filled.tieback_proration_rate,
    filled.card_proration_rate,
    filled.char_yrblt,
    filled.char_building_sf,
    filled.char_unit_sf,
    filled.char_bedrooms,
    filled.char_half_baths,
    filled.char_full_baths,

    -- Count of non-unit PINs by pin10
    SUM(CASE
        WHEN (
            filled.cdu IN ('GR', 'PS')
            OR (
                SUBSTR(filled.unitno, 1, 1) = 'P'
                AND SUBSTR(filled.unitno, 1, 2) != 'PH'
            )
            OR SUBSTR(filled.unitno, 1, 3) = 'GAR'
            OR filled.note = 'PARKING/STORAGE/COMMON UNIT'
            OR filled.parking_pin = TRUE
            -- If a unit's percent of the declaration is less than half of
            -- what it would be if all units had an equal share, AV limited
            OR (
                filled.tiebldgpct < (50 / filled.building_pins)
                AND vph.oneyr_pri_board_tot BETWEEN 10 AND 5000
            )
            OR vph.oneyr_pri_board_tot BETWEEN 10 AND 1000
            OR nonlivable.flag = 'negative pred'
        )
        AND (nonlivable.flag != 'questionable' OR nonlivable.flag IS NULL)
            THEN 1
        ELSE 0
    END)
        OVER (PARTITION BY filled.pin10, filled.year)
        AS char_building_non_units,

    filled.building_pins AS char_building_pins,
    aggregate_land.total_building_land_sf AS char_land_sf,
    filled.cdu,
    filled.note,
    filled.unitno,
    filled.bldg_is_mixed_use,
    vph.oneyr_pri_board_tot,
    COALESCE((
        filled.cdu IN ('GR', 'PS')
        OR (
            SUBSTR(filled.unitno, 1, 1) = 'P'
            AND SUBSTR(filled.unitno, 1, 2) != 'PH'
        )
        OR SUBSTR(filled.unitno, 1, 3) = 'GAR'
        OR filled.note = 'PARKING/STORAGE/COMMON UNIT'
        OR filled.parking_pin = TRUE
        -- If a unit's percent of the declaration is less than half of
        -- what it would be if all units had an equal share, AV limited
        OR (
            filled.tiebldgpct < (50 / filled.building_pins)
            AND vph.oneyr_pri_board_tot BETWEEN 10 AND 5000
        )
        OR vph.oneyr_pri_board_tot BETWEEN 10 AND 1000
        OR nonlivable.flag = 'negative pred'
    )
    AND (nonlivable.flag != 'questionable' OR nonlivable.flag IS NULL),
    FALSE) AS is_parking_space,
    CASE
        WHEN nonlivable.flag = 'questionable' THEN NULL
        WHEN
            nonlivable.flag = 'negative pred'
            THEN 'model predicted negative value'
        WHEN filled.note = 'PARKING/STORAGE/COMMON UNIT'
            OR filled.parking_pin = TRUE
            THEN 'identified by valuations as non-unit'
        WHEN filled.cdu IN ('GR', 'PS') THEN 'cdu'
        WHEN (
            SUBSTR(filled.unitno, 1, 1) = 'P'
            AND SUBSTR(filled.unitno, 1, 2) != 'PH'
        )
        OR SUBSTR(filled.unitno, 1, 3) = 'GAR' THEN 'unit number'
        -- If a unit's percent of the declaration is less than half of what
        -- it would be if all units had an equal share, AV limited
        WHEN
            (
                filled.tiebldgpct < (50 / filled.building_pins)
                AND vph.oneyr_pri_board_tot BETWEEN 10 AND 5000
            )
            THEN 'declaration percent'
        WHEN
            vph.oneyr_pri_board_tot BETWEEN 10 AND 1000
            THEN 'prior value'
    END AS parking_space_flag_reason,
    COALESCE(vph.oneyr_pri_board_tot < 10, FALSE) AS is_common_area,
    COALESCE(nonlivable.flag = 'questionable', FALSE)
        AS is_question_garage_unit,
    COALESCE(nonlivable.flag = 'negative pred', FALSE) AS is_negative_pred,
    aggregate_land.pin_is_multiland,
    aggregate_land.pin_num_landlines

FROM filled
LEFT JOIN aggregate_land
    ON filled.pin = aggregate_land.parid
    AND filled.year = aggregate_land.taxyr
LEFT JOIN {{ ref('default.vw_pin_history') }} AS vph
    ON filled.pin = vph.pin
    AND filled.year = vph.year
LEFT JOIN {{ source('ccao', 'pin_nonlivable') }} AS nonlivable
    ON filled.pin = nonlivable.pin
