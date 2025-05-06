-- Copy of default.vw_pin_address that feeds the "Parcel Addresses" open data
-- asset.
-- Some columns from the feeder view may not be present in this view.

WITH feeder AS (
    SELECT
        pin,
        pin10,
        CAST(year AS INT) AS feeder_year,
        prop_address_full,
        prop_address_city_name,
        prop_address_state,
        prop_address_zipcode_1,
        mail_address_name,
        mail_address_full,
        mail_address_city_name,
        mail_address_state,
        mail_address_zipcode_1
    FROM {{ ref('default.vw_pin_address') }}
),

{{ open_data_rows_to_delete(feeder) }}
