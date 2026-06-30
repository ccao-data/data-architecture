WITH flags AS (
    SELECT
        pin,
        year,
        township_code,
        COALESCE(char_half_baths > 3, FALSE) AS flag_half_baths,
        COALESCE(char_full_baths > 4, FALSE) AS flag_full_baths,
        COALESCE(char_bedrooms > 4, FALSE) AS flag_bedrooms,
        COALESCE(
            char_unit_sf NOT BETWEEN 200 AND 10000
            OR char_unit_sf > char_building_sf,
            FALSE
        ) AS flag_unit_sf,
        COALESCE(
            char_building_sf NOT BETWEEN 500 AND 2000000
            OR char_building_sf < char_unit_sf,
            FALSE
        ) AS flag_building_sf,
        -- PLACEHOLDER VALS
        COALESCE(char_land_sf > 100000, FALSE) AS flag_land_sf,
        COALESCE(char_building_pins > 1, FALSE) AS flag_building_pins,
        COALESCE(char_building_pins - char_building_non_units > 1, FALSE)
            AS flag_building_units,
        COALESCE(char_building_non_units > 0, FALSE) AS flag_building_non_units,
        COALESCE(char_yrblt NOT BETWEEN 1800 AND 2023, FALSE) AS flag_yrblt,
        COALESCE(bldg_is_mixed_use, FALSE) AS flag_bldg_is_mixed_use
    FROM {{ ref('default.vw_pin_condo_char') }}
    WHERE year = (SELECT MAX(year) FROM {{ ref('default.vw_pin_condo_char') }})
)

SELECT
    *,
    CONCAT_WS(
        ', ',
        CASE WHEN flag_half_baths THEN 'More than 3 Half Baths' END,
        CASE WHEN flag_full_baths THEN 'More than 4 Full Baths' END,
        CASE WHEN flag_bedrooms THEN 'More than 4 Bedrooms' END,
        CASE
            WHEN flag_unit_sf THEN 'Unit SF not between 200 and 10,000'
        END,
        CASE
            WHEN
                flag_building_sf
                THEN 'Building SF not between 500 and 2,000,000'
        END,
        CASE
            WHEN flag_land_sf THEN 'Land SF not between 500 and 2,000,000'
        END,
        CASE
            WHEN flag_building_pins THEN 'Building has multiple PINs'
        END,
        CASE
            WHEN flag_building_units THEN 'Building has multiple units'
        END,
        CASE
            WHEN flag_building_non_units THEN 'Building has non-unit spaces'
        END,
        CASE
            WHEN flag_yrblt THEN 'Year Built not between 1800 and 2023'
        END,
        CASE
            WHEN flag_bldg_is_mixed_use THEN 'Building is mixed use'
        END
    ) AS flag_comments
FROM flags
