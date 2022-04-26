/**
View containing cleaned, filled data for condo modeling. Missing data is
filled with the following steps:

All historical data is filled FORWARD in time, i.e. data from 2020 fills
2021 as long as the data isn't something which frequently changes
**/

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
-- Valuations has detected some PINs that shouldn't be considered parking spaces
questionable_gr AS (SELECT pin, TRUE AS is_question_garage_unit FROM ccao.pin_questionable_garage_units),
-- For some reason PINs can have cur != 'Y' in the current year even when there's only one row
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
-- Prior year AV, used to help fing parking spaces and common areas
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
    SELECT DISTINCT * FROM ( -- Distinct because oby and comdat contain multiple cards for a few condos
        SELECT

            pardat.parid AS pin,
            CASE
                WHEN pardat.class IN ('299', '2-99') THEN oby.card
                WHEN pardat.class = '399' THEN comdat.card
            END AS card,
            SUBSTR(pardat.parid, 1, 10) AS pin10,
            pardat.class,
            pardat.taxyr AS year,
            CASE
                WHEN pardat.class IN ('299', '2-99') THEN oby.user16
                WHEN pardat.class = '399' THEN comdat.user16
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
            pin_condo_char.parking_pin,
            unitno,
            tiebldgpct,
            pardat.note2 AS note,
            CASE WHEN SUM(CASE WHEN pardat.class NOT IN ('299', '2-99', '399') THEN 1 ELSE 0 END)
                OVER (PARTITION BY SUBSTR(pardat.parid, 1, 10), pardat.taxyr) > 0 THEN true ELSE false END
                AS bldg_is_mixed_use

        FROM iasworld.pardat

        -- Left joins because pardat contains both 299s & 399s (oby and comdat do not)
        -- and pin_condo_char doesn't contain all condos
        LEFT JOIN oby_filtered oby
        ON pardat.parid = oby.parid
            AND pardat.taxyr = oby.taxyr
        LEFT JOIN comdat_filtered comdat
        ON pardat.parid = comdat.parid
            AND pardat.taxyr = comdat.taxyr
        LEFT JOIN ccao.pin_condo_char
        ON pardat.parid = pin_condo_char.pin
            AND pardat.taxyr = pin_condo_char.year
    )

    WHERE class IN ('299', '2-99', '399')
),
filled AS (
    -- Backfilling data since it's rarely updated
    SELECT
        pin,
        pin10,
        card,
        class,
        year,
        char_yrblt,
        -- CDUs/notes are not well-maintained year-to-year,
        CASE
            WHEN cdu IS NULL
                THEN LAST_VALUE(cdu) IGNORE NULLS
                OVER (PARTITION BY pin ORDER BY year DESC)
            ELSE cdu
        END AS cdu,
        CASE
            WHEN note IS NULL
                THEN LAST_VALUE(note) IGNORE NULLS
                OVER (PARTITION BY pin ORDER BY year DESC)
            ELSE note
        END AS note,
        -- Characteristics data gathered from MLS by valuations
        CASE
            WHEN char_building_sf IS NULL
                THEN LAST_VALUE(char_building_sf) IGNORE NULLS
                OVER (PARTITION BY pin ORDER BY year DESC)
            ELSE char_building_sf
        END AS char_building_sf,
        CASE
            WHEN char_unit_sf IS NULL
                THEN LAST_VALUE(char_unit_sf) IGNORE NULLS
                OVER (PARTITION BY pin ORDER BY year DESC)
            ELSE char_unit_sf
        END AS char_unit_sf,
        CASE
            WHEN char_bedrooms IS NULL
                THEN LAST_VALUE(char_bedrooms) IGNORE NULLS
                OVER (PARTITION BY pin ORDER BY year DESC)
            ELSE char_bedrooms
        END AS char_bedrooms,
        CASE
            WHEN parking_pin IS NULL
                THEN LAST_VALUE(parking_pin) IGNORE NULLS
                OVER (PARTITION BY pin ORDER BY year DESC)
            ELSE parking_pin
        END AS parking_pin,
        unitno,
        tiebldgpct,
        bldg_is_mixed_use,
        COUNT(*)
        OVER (PARTITION BY pin10, year)
            AS building_pins
    FROM chars

    -- Some PINs show up up with different yrblt across lline (within year)
    WHERE char_yrblt = max_yrblt
)

SELECT

    filled.pin,
    filled.pin10,
    filled.card,
    filled.year,
    CASE WHEN filled.class = '2-99'
        THEN '299'
        ELSE filled.class END AS class,
    filled.char_yrblt,
    filled.char_building_sf,
    filled.char_unit_sf,
    filled.char_bedrooms,

    -- Count of non-unit PINs by pin10
    sum(CASE
        WHEN (filled.cdu = 'GR'
            OR (SUBSTR(filled.unitno, 1, 1) = 'P' AND filled.unitno != 'PH')
            OR SUBSTR(filled.unitno, 1, 3) = 'GAR'
            OR filled.note = 'PARKING/STORAGE/COMMON UNIT'
            OR filled.parking_pin = TRUE
            -- If a unit's percent of the declaration is less than half of what it would be if all units had an equal share, AV limited
            OR (filled.tiebldgpct < (50 / filled.building_pins) AND prior_values.oneyr_pri_board_tot BETWEEN 10 AND 5000)
            OR prior_values.oneyr_pri_board_tot BETWEEN 10 AND 1000)
            AND questionable_gr.is_question_garage_unit != TRUE
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
            OR (SUBSTR(filled.unitno, 1, 1) = 'P' AND filled.unitno != 'PH')
            OR SUBSTR(filled.unitno, 1, 3) = 'GAR'
            OR filled.note = 'PARKING/STORAGE/COMMON UNIT'
            OR filled.parking_pin = TRUE
            -- If a unit's percent of the declaration is less than half of what it would be if all units had an equal share, AV limited
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
        -- If a unit's percent of the declaration is less than half of what it would be if all units had an equal share, AV limited
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
INNER JOIN aggregate_land
ON filled.pin = aggregate_land.parid
    AND filled.year = aggregate_land.taxyr
LEFT JOIN prior_values
ON filled.pin = prior_values.pin
    AND filled.year = prior_values.year
LEFT JOIN questionable_gr
ON filled.pin = questionable_gr.pin
