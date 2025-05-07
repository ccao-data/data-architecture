-- Copy of default.vw_pin_exempt that feeds the "Property Tax-Exempt Parcels"
-- open data asset.
-- Some columns from the feeder view may not be present in this view.

SELECT
    feeder.pin,
    feeder.township_name,
    feeder.township_code,
    feeder.owner_name,
    feeder.owner_num,
    feeder.class,
    feeder.property_address,
    feeder.property_city,
    feeder.lon,
    feeder.lat,
    {{ open_data_columns(card=false) }}
FROM {{ ref('default.vw_pin_exempt') }} AS feeder
{{ open_data_rows_to_delete(card=false) }}
