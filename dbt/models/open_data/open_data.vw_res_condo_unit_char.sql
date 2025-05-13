-- Copy of default.vw_pin_condo_char that feeds the "Residential Condominium
-- Unit Characteristics" open data asset.
-- Some columns from the feeder view may not be present in this view.

SELECT
    feeder.pin,
    feeder.pin10,
    feeder.card,
    feeder.class,
    feeder.township_code,
    feeder.tieback_key_pin,
    feeder.tieback_proration_rate,
    feeder.card_proration_rate,
    feeder.char_yrblt,
    feeder.char_building_sf,
    feeder.char_unit_sf,
    feeder.char_bedrooms,
    feeder.char_half_baths,
    feeder.char_full_baths,
    feeder.char_building_non_units,
    feeder.char_building_pins,
    feeder.char_land_sf,
    feeder.cdu,
    feeder.bldg_is_mixed_use,
    feeder.is_parking_space,
    feeder.is_common_area,
    feeder.pin_is_multiland,
    feeder.pin_num_landlines,
    {{ open_data_columns() }}
FROM {{ ref('default.vw_pin_condo_char') }} AS feeder
{{ open_data_rows_to_delete(condo=true) }}
