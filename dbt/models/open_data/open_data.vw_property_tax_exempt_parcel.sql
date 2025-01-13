-- Copy of default.vw_pin_exempt that feeds the "Property Tax-Exempt Parcels"
-- open data asset.
SELECT
    CONCAT(pin, year) AS row_id,
    pin,
    year,
    township_name,
    township_code,
    owner_name,
    owner_num,
    class,
    property_address,
    property_city,
    lon,
    lat
FROM {{ ref('default.vw_pin_exempt') }}
