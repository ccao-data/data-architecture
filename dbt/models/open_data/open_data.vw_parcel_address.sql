-- Copy of default.vw_pin_address that feeds the "Parcel Addresses" open data
-- asset.
SELECT
    CONCAT(pin, year) AS row_id,
    pin,
    pin10,
    year,
    --prop_address_prefix,
    --prop_address_street_number,
    --prop_address_street_dir,
    --prop_address_street_name,
    --prop_address_suffix_1,
    --prop_address_suffix_2,
    --prop_address_unit_prefix,
    --prop_address_unit_number,
    prop_address_full,
    prop_address_city_name,
    prop_address_state,
    prop_address_zipcode_1,
    --prop_address_zipcode_2,
    mail_address_name,
    mail_address_full,
    mail_address_city_name,
    mail_address_state,
    mail_address_zipcode_1
    --mail_address_zipcode_2
FROM {{ ref('default.vw_pin_address') }}
