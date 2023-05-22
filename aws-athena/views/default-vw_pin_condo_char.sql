/*
View containing cleaned, filled data for condo modeling. Missing data is
filled as follows:

Condo characteristics are filled with whatever the most recent non-NULL
value is. This assumes that new condo data is more accurate than older
data, not that it represents a change in a unit's characteristics. This
should only be the case while condo characteristics are pulled from excel
workbooks rather than iasWorld.
*/

CREATE OR REPLACE VIEW default.vw_pin_condo_char
AS
WITH aggregate_land AS (
    SELECT
        parid,
        taxyr,
        CASE
            WHEN COUNT(*) > 1 THEN TRUE
            ELSE FALSE
        END AS pin_is_multiland,
        COUNT(*) AS pin_num_landlines,
        SUM(sf) AS total_building_land_sf

    FROM iasworld.land

    GROUP BY parid,taxyr
    ),
-- Valuations-provided PINs that shouldn't be considered parking spaces
questionable_gr AS (
    SELECT
        pin,
        TRUE AS is_question_garage_unit
    FROM ccao.pin_questionable_garage_units
),
-- For some reason PINs can have cur != 'Y' in the current year even
-- when there's only one row
oby_filtered AS (
    SELECT * FROM (
        SELECT
            *,
            SUM(CASE WHEN CUR = 'Y' THEN 1 ELSE 0 END) OVER (PARTITION BY parid, taxyr) cur_count,
            ROW_NUMBER() OVER (PARTITION BY parid, taxyr ORDER BY wen DESC) AS row_no
        FROM iasworld.oby
        WHERE class IN ('299', '2-99', '200')
    )
    WHERE (cur = 'Y' OR (cur_count = 0 and row_no = 1))
),
comdat_filtered AS (
    SELECT * FROM (
        SELECT
            *,
            SUM(CASE WHEN CUR = 'Y' THEN 1 ELSE 0 END) OVER (PARTITION BY parid, taxyr) cur_count,
            ROW_NUMBER() OVER (PARTITION BY parid, taxyr ORDER BY wen DESC) AS row_no
        FROM iasworld.comdat
        WHERE class = '399'
    )
    WHERE (cur = 'Y' OR (cur_count = 0 and row_no = 1))
),
-- Prior year AV, used to help find parking spaces and common areas
prior_values AS (
    SELECT

        parid as pin,
        CAST(CAST(taxyr AS int) + 1 AS varchar) AS year,
        MAX(
            CASE
                WHEN procname = 'BORVALUE' AND taxyr < '2020' THEN ovrvalasm3
                WHEN procname = 'BORVALUE' AND valclass IS NULL AND taxyr >= '2020' THEN valasm3
                ELSE NULL
            END
            ) AS oneyr_pri_board_tot

    FROM iasworld.asmt_all

    WHERE class IN ('299', '2-99', '399')
    GROUP BY parid, taxyr
    ),
-- All characteristics associated with condos in the OBY (299s)/COMDAT (399s) tables
chars AS (
    -- Distinct because oby and comdat contain multiple cards for a few condos
    SELECT DISTINCT * FROM (
        SELECT
            pardat.parid AS pin,
            CASE
                WHEN pardat.class IN ('299', '2-99') THEN oby.card
                WHEN pardat.class = '399' THEN comdat.card
            END AS card,
            -- Proration related fields from PARDAT
            pardat.tieback AS tieback_key_pin,
            CASE
                WHEN pardat.tiebldgpct IS NOT NULL THEN pardat.tiebldgpct / 100.0
                WHEN pardat.tiebldgpct IS NULL AND pardat.class IN ('299', '2-99', '399') THEN 0
            ELSE 1.0 END AS tieback_proration_rate,
            CASE
                WHEN pardat.class IN ('299', '2-99') THEN CAST(oby.user20 AS double) / 100.0
                WHEN pardat.class = '399' THEN CAST(comdat.user24 AS double) / 100.0
            END AS card_protation_rate,
            lline,
            SUBSTR(pardat.parid, 1, 10) AS pin10,
            pardat.class,
            pardat.taxyr AS year,
            substr(TAXDIST, 1, 2) AS township_code,
            CASE
                WHEN pardat.class IN ('299', '2-99') THEN oby.user16
                WHEN pardat.class = '399' AND p3gu.user16 IS NULL THEN comdat.user16
                WHEN pardat.class = '399' AND p3gu.user16 IS NOT NULL THEN p3gu.user16
            END AS cdu,
            -- Very rarely use 'effyr' rather than 'yrblt' when 'yrblt' is NULL
            CASE
                WHEN pardat.class IN ('299', '2-99') THEN COALESCE(oby.yrblt, oby.effyr, comdat.yrblt, comdat.effyr)
                WHEN pardat.class = '399' THEN COALESCE(comdat.yrblt, comdat.effyr, oby.yrblt, oby.effyr)
            END AS char_yrblt,
            MAX(
                CASE
                    WHEN pardat.class IN ('299', '2-99') THEN COALESCE(oby.yrblt, oby.effyr, comdat.yrblt, comdat.effyr)
                    WHEN pardat.class = '399' THEN COALESCE(comdat.yrblt, comdat.effyr, oby.yrblt, oby.effyr)
            END
            )
            OVER (PARTITION BY pardat.parid, pardat.taxyr)
            AS max_yrblt,
            CAST(ROUND(pin_condo_char.building_sf, 0) AS int) AS char_building_sf,
            CAST(ROUND(pin_condo_char.unit_sf, 0) AS int) AS char_unit_sf,
            CAST(pin_condo_char.bedrooms AS int) AS char_bedrooms,
            CAST(pin_condo_char.half_baths AS int) AS char_half_baths,
            CAST(pin_condo_char.full_baths AS int) AS char_full_baths,
            pin_condo_char.parking_pin,
            pardat.unitno,
            tiebldgpct,
            pardat.note2 AS note,
            CASE WHEN SUM(CASE WHEN pardat.class NOT IN ('299', '2-99', '399') THEN 1 ELSE 0 END)
                OVER (PARTITION BY SUBSTR(pardat.parid, 1, 10), pardat.taxyr) > 0 THEN true ELSE false END
                AS bldg_is_mixed_use

        FROM iasworld.pardat

        -- Left joins because pardat contains both 299s & 399s (oby and comdat
        -- do not) and pin_condo_char doesn't contain all condos
        LEFT JOIN oby_filtered oby
            ON pardat.parid = oby.parid
            AND pardat.taxyr = oby.taxyr
        LEFT JOIN comdat_filtered comdat
            ON pardat.parid = comdat.parid
            AND pardat.taxyr = comdat.taxyr
        LEFT JOIN ccao.pin_condo_char
            ON pardat.parid = pin_condo_char.pin
            AND pardat.taxyr = pin_condo_char.year
        LEFT JOIN iasworld.legdat
            ON pardat.parid = legdat.parid
            AND pardat.taxyr = legdat.taxyr
        LEFT JOIN ccao.pin_399_garage_units p3gu
            ON pardat.parid = p3gu.parid
            AND pardat.taxyr = p3gu.taxyr
    )

    WHERE class IN ('299', '2-99', '399')
),
filled AS (
    -- Backfilling data since it's rarely updated
    SELECT
        pin,
        pin10,
        card,
        lline,
        class,
        year,
        township_code,
        char_yrblt,
        -- CDUs/notes are not well-maintained year-to-year
        -- but CDU CAN be updated to NULL
        FIRST_VALUE(cdu)
            OVER (PARTITION BY pin ORDER BY year DESC)
            AS cdu,
        CASE
            WHEN note IS NULL
                THEN LAST_VALUE(note) IGNORE NULLS
                OVER (PARTITION BY pin ORDER BY year DESC)
            ELSE note
        END AS note,
        -- Characteristics data gathered from MLS by valuations
        FIRST_VALUE(char_building_sf)
            OVER (PARTITION BY pin ORDER BY case when char_building_sf IS NOT NULL THEN year ELSE '0' END DESC)
        AS char_building_sf,
        FIRST_VALUE(char_unit_sf)
            OVER (PARTITION BY pin ORDER BY case when char_unit_sf IS NOT NULL THEN year ELSE '0' END DESC)
        AS char_unit_sf,
        FIRST_VALUE(char_bedrooms)
            OVER (PARTITION BY pin ORDER BY case when char_bedrooms IS NOT NULL THEN year ELSE '0' END DESC)
        AS char_bedrooms,
        FIRST_VALUE(char_half_baths)
            OVER (PARTITION BY pin ORDER BY case when char_half_baths IS NOT NULL THEN year ELSE '0' END DESC)
        AS char_half_baths,
        FIRST_VALUE(char_full_baths)
            OVER (PARTITION BY pin ORDER BY case when char_full_baths IS NOT NULL THEN year ELSE '0' END DESC)
        AS char_full_baths,
        FIRST_VALUE(parking_pin)
            OVER (PARTITION BY pin ORDER BY case when parking_pin IS NOT NULL THEN year ELSE '0' END DESC)
        AS parking_pin,
        unitno,
        tiebldgpct,
        tieback_key_pin,
        tieback_proration_rate,
        card_protation_rate,
        bldg_is_mixed_use,
        COUNT(*)
        OVER (PARTITION BY pin10, year)
            AS building_pins
    FROM chars

)

SELECT DISTINCT

    filled.pin,
    filled.pin10,
    filled.card,
    filled.lline,
    filled.year,
    CASE WHEN filled.class = '2-99'
        THEN '299'
        ELSE filled.class END AS class,
    filled.township_code,
    -- Count pin rather than lline here since lline can be null. It shouldn't
    -- be, but some condo PINs exist in PARDAT and not OBY
    CASE
        WHEN COUNT(filled.pin) OVER (PARTITION BY filled.pin, filled.year) > 1 THEN TRUE
        ELSE FALSE
    END pin_is_multilline,
    COUNT(filled.pin) OVER (PARTITION BY filled.pin, filled.year) AS pin_num_lline,
    tieback_key_pin,
    tieback_proration_rate,
    card_protation_rate,
    filled.char_yrblt,
    filled.char_building_sf,
    filled.char_unit_sf,
    filled.char_bedrooms,
    filled.char_half_baths,
    filled.char_full_baths,

    -- Count of non-unit PINs by pin10
    sum(CASE
        WHEN (filled.cdu = 'GR'
            OR (SUBSTR(filled.unitno, 1, 1) = 'P' AND SUBSTR(filled.unitno, 1, 2) != 'PH')
            OR SUBSTR(filled.unitno, 1, 3) = 'GAR'
            OR filled.note = 'PARKING/STORAGE/COMMON UNIT'
            OR filled.parking_pin = TRUE
            -- If a unit's percent of the declaration is less than half of
            -- what it would be if all units had an equal share, AV limited
            OR (filled.tiebldgpct < (50 / filled.building_pins) AND prior_values.oneyr_pri_board_tot BETWEEN 10 AND 5000)
            OR prior_values.oneyr_pri_board_tot BETWEEN 10 AND 1000)
            AND questionable_gr.is_question_garage_unit IS NULL
        THEN 1
        ELSE 0 END)
        OVER (PARTITION BY filled.pin10, filled.year)
        AS char_building_non_units,

    filled.building_pins as char_building_pins,
    filled.tiebldgpct as char_tiebldgpct,
    total_building_land_sf as char_land_sf,
    filled.cdu,
    filled.note,
    filled.unitno,
    filled.bldg_is_mixed_use,
    prior_values.oneyr_pri_board_tot,

    CASE
        WHEN (filled.cdu = 'GR'
            OR (SUBSTR(filled.unitno, 1, 1) = 'P' AND SUBSTR(filled.unitno, 1, 2) != 'PH')
            OR SUBSTR(filled.unitno, 1, 3) = 'GAR'
            OR filled.note = 'PARKING/STORAGE/COMMON UNIT'
            OR filled.parking_pin = TRUE
            -- If a unit's percent of the declaration is less than half of
            -- what it would be if all units had an equal share, AV limited
            OR (filled.tiebldgpct < (50 / filled.building_pins) AND prior_values.oneyr_pri_board_tot BETWEEN 10 AND 5000)
            OR prior_values.oneyr_pri_board_tot BETWEEN 10 AND 1000)
            AND questionable_gr.is_question_garage_unit IS NULL
        THEN TRUE
        ELSE FALSE
    END AS is_parking_space,
    CASE
        WHEN questionable_gr.is_question_garage_unit = TRUE THEN NULL
        WHEN filled.note = 'PARKING/STORAGE/COMMON UNIT' OR filled.parking_pin = TRUE
          THEN 'identified by valuations as non-unit'
        WHEN filled.cdu = 'GR' THEN 'cdu'
        WHEN (SUBSTR(filled.unitno, 1, 1) = 'P' AND filled.unitno != 'PH')
            OR SUBSTR(filled.unitno, 1, 3) = 'GAR' THEN 'unit number'
        -- If a unit's percent of the declaration is less than half of what
        -- it would be if all units had an equal share, AV limited
        WHEN (filled.tiebldgpct < (50 / filled.building_pins) AND prior_values.oneyr_pri_board_tot BETWEEN 10 AND 5000) THEN 'declaration percent'
        WHEN prior_values.oneyr_pri_board_tot BETWEEN 10 AND 1000 THEN 'prior value'
        ELSE NULL
    END AS parking_space_flag_reason,
    CASE
        WHEN prior_values.oneyr_pri_board_tot < 10
        THEN TRUE
        ELSE FALSE
    END AS is_common_area,
    questionable_gr.is_question_garage_unit,

    pin_is_multiland,
    pin_num_landlines

FROM filled
LEFT JOIN aggregate_land
ON filled.pin = aggregate_land.parid
    AND filled.year = aggregate_land.taxyr
LEFT JOIN prior_values
ON filled.pin = prior_values.pin
    AND filled.year = prior_values.year
LEFT JOIN questionable_gr
ON filled.pin = questionable_gr.pin
