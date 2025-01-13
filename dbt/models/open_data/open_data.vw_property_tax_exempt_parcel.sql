-- View to collect currently exempt properties
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
