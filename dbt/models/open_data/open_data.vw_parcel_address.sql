-- Copy of default.vw_pin_address that feeds the "Parcel Addresses" open data
-- asset.
-- Some columns from the feeder view may not be present in this view.

SELECT
    feeder.pin,
    feeder.pin10,
    feeder.prop_address_full,
    feeder.prop_address_city_name,
    feeder.prop_address_state,
    feeder.prop_address_zipcode_1,
    feeder.mail_address_name,
    feeder.mail_address_full,
    feeder.mail_address_city_name,
    feeder.mail_address_state,
    feeder.mail_address_zipcode_1,
    {{ open_data_columns(card=false) }}
FROM {{ ref('default.vw_pin_address') }} AS feeder
{{ open_data_rows_to_delete(card=false, allow_999=false) }}
