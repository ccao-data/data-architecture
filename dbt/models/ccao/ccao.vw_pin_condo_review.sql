WITH flags AS (
    SELECT
        pin,
        year,
        township_code,
        COALESCE(char_half_baths > 3, FALSE) AS flag_half_baths,
        COALESCE(char_full_baths > 4, FALSE) AS flag_full_baths,
        COALESCE(char_bedrooms > 4, FALSE) AS flag_bedrooms,
        COALESCE(
            char_unit_sf NOT BETWEEN 500 AND 3000
            OR char_unit_sf > char_building_sf,
            FALSE
        ) AS flag_unit_sf,
        COALESCE(
            char_building_sf NOT BETWEEN 2500 AND 100000
            OR char_building_sf < char_unit_sf,
            FALSE
        ) AS flag_building_sf,
        COALESCE(char_land_sf NOT BETWEEN 2500 AND 100000, FALSE)
            AS flag_land_sf,
        COALESCE(char_building_pins - char_building_non_units = 0, FALSE)
            AS flag_no_livable_units,
        COALESCE(char_yrblt NOT BETWEEN 1880 AND YEAR(CURRENT_DATE), FALSE)
            AS flag_yrblt,
        COALESCE(bldg_is_mixed_use, FALSE) AS flag_bldg_is_mixed_use
    FROM {{ ref('default.vw_pin_condo_char') }}
    WHERE year = (SELECT MAX(year) FROM {{ ref('default.vw_pin_condo_char') }})
),

comments AS (
    SELECT
        *,
        NULLIF(CONCAT_WS(
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
                    THEN 'Building SF not between 2,500 and 100,000'
            END,
            CASE
                WHEN flag_land_sf THEN 'Land SF not between 2,500 and 100,000'
            END,
            CASE
                WHEN flag_no_livable_units THEN 'Building has no livable units'
            END,
            CASE
                WHEN
                    flag_yrblt
                    THEN CONCAT(
                        'Year Built not between 1880 and ', YEAR(CURRENT_DATE)
                    )
            END,
            CASE
                WHEN flag_bldg_is_mixed_use THEN 'Building is mixed use'
            END
        ), '') AS flag_comments
    FROM flags
)

SELECT * FROM comments WHERE flag_comments IS NOT NULL
