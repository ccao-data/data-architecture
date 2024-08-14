-- View collects condo parcels that have been identified as nonlivable but have
-- associated characteristics provided by data integrity.
SELECT
    condos.pin,
    condos.year,
    condos.class,
    towns.township_name,
    condos.tieback_proration_rate,
    condos.char_unit_sf,
    condos.char_half_baths,
    condos.char_full_baths,
    condos.note,
    condos.unitno,
    condos.is_parking_space,
    condos.parking_space_flag_reason,
    condos.is_common_area
FROM {{ ref('default.vw_pin_condo_char') }} AS condos
LEFT JOIN {{ source('spatial', 'township') }} AS towns
    ON condos.township_code = towns.township_code
WHERE
    (condos.is_common_area OR condos.is_parking_space)
    AND (condos.char_bedrooms IS NOT NULL OR condos.char_full_baths IS NOT NULL)
