SELECT
    pin,
    township_code,
    COALESCE(char_half_baths = 5, FALSE) AS flag_half_baths,
    COALESCE(char_full_baths = 40, FALSE) AS flag_full_baths,
    COALESCE(char_bedrooms >= 9, FALSE) AS flag_bedrooms,
    COALESCE(
        char_unit_sf NOT BETWEEN 200 AND 10000
        OR char_unit_sf > char_building_sf,
        FALSE
    ) AS flag_unit_sf,
    char_building_pins AS flag_building_pins,
    char_building_pins - char_building_non_units AS flag_building_units,
    char_building_non_units AS flag_building_non_units,
    COALESCE(
        char_building_sf NOT BETWEEN 500 AND 2000000
        OR char_building_sf < char_unit_sf,
        FALSE
    ) AS flag_building_sf,
    char_yrblt AS flag_yrblt,
    bldg_is_mixed_use AS flag_bldg_is_mixed_use,
    char_land_sf AS flag_land_sf,
    '' AS flag_comments
FROM {{ ref('vw_pin_condo_char') }}
WHERE year = (SELECT MAX(year) FROM {{ ref('vw_pin_condo_char') }})
