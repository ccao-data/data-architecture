/*
View containing cleaned, filled data for condo modeling. Missing data is
filled as follows:

Condo characteristics are filled with whatever the most recent non-NULL
value is. This assumes that new condo data is more accurate than older
data, not that it represents a change in a unit's characteristics. This
should only be the case while condo characteristics are pulled from excel
workbooks rather than iasWorld.
*/
SELECT
    CONCAT(pin, year) AS row_id,
    pin,
    pin10,
    card,
    --lline,
    year,
    class,
    township_code,
    --pin_is_multilline,
    --pin_num_lline,
    tieback_key_pin,
    tieback_proration_rate,
    card_proration_rate,
    char_yrblt,
    char_building_sf,
    char_unit_sf,
    char_bedrooms,
    char_half_baths AS num_half_baths,
    char_full_baths AS num_full_baths,
    char_building_non_units,
    char_building_pins,
    char_land_sf,
    cdu,
    --note,
    --unitno,
    bldg_is_mixed_use,
    oneyr_pri_board_tot,
    --oneyr_pri_board_tot,
    is_parking_space,
    --parking_space_flag_reason,
    is_common_area,
    --is_question_garage_unit,
    --is_negative_pred,
    pin_is_multiland,
    pin_num_landlines
FROM {{ ref('default.vw_pin_condo_char') }}
