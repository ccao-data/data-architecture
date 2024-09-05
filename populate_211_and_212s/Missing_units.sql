SELECT
    rc.pin,
    pa.prop_address_street_name,
    pa.prop_address_street_number,
    pa.prop_address_city_name
FROM vw_card_res_char AS rc
INNER JOIN vw_pin_address AS pa
    ON rc.pin = pa.pin
WHERE
    (rc.char_apts = '0' OR rc.char_apts IS NULL OR rc.char_apts = '6')
    AND rc.year = '2023'
    AND (rc.class = '211' OR rc.class = '212')
