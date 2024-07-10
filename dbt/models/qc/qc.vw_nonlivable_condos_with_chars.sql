-- View collects are condo parcels from the most recent year of data that have
-- been identified as non-livable but have associated characteristics provided
-- by data integrity.
SELECT
    pin,
    year
FROM {{ ref('default.vw_pin_condo_char') }}
WHERE
    (is_common_area OR is_parking_space)
    AND (char_bedrooms IS NOT NULL OR char_full_baths IS NOT NULL)
    AND year = (SELECT MAX(year) FROM {{ ref('default.vw_pin_condo_char') }})
