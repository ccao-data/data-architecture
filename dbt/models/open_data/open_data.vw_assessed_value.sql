-- Copy of default.vw_pin_history that feeds the "Assessed Values" open data
-- asset.
-- Some columns from the feeder view may not be present in this view.

SELECT
    feeder.pin,
    feeder.class,
    feeder.township_code,
    feeder.township_name,
    feeder.nbhd,
    feeder.mailed_bldg,
    feeder.mailed_land,
    feeder.mailed_tot,
    feeder.certified_bldg,
    feeder.certified_land,
    feeder.certified_tot,
    feeder.board_bldg,
    feeder.board_land,
    feeder.board_tot,
    {{ open_data_columns(card=false) }}
FROM {{ ref('default.vw_pin_history') }} AS feeder
{{ open_data_rows_to_delete(card=false) }}
