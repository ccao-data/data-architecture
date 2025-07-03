-- Copy of default.vw_pin_condo_char that feeds the "Residential Condominium
-- Unit Characteristics" open data asset.
-- Some columns from the feeder view may not be present in this view.

SELECT
    CONCAT(pin, year) AS row_id,
    pin,
    pin10,
    card,
    CAST(year AS INT) AS year,
    class,
    township_code,
    tieback_key_pin,
    tieback_proration_rate,
    card_proration_rate,
    char_yrblt,
    char_building_sf,
    char_unit_sf,
    char_bedrooms,
    char_half_baths,
    char_full_baths,
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
