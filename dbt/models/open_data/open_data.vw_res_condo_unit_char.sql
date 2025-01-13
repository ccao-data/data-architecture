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
    pin,
    pin10,
    card,
    year,
    class,
    township_code,
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
    bldg_is_mixed_use,
    is_parking_space,
    is_common_area,
    pin_is_multiland,
    pin_num_landlines
FROM {{ ref('default.vw_pin_condo_char') }}
