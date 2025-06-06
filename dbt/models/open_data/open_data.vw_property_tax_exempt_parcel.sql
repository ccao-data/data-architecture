-- Copy of default.vw_pin_exempt that feeds the "Property Tax-Exempt Parcels"
-- open data asset.
-- Some columns from the feeder view may not be present in this view.

SELECT
    CONCAT(pin, year) AS row_id,
    pin,
    CAST(year AS INT) AS year,
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
