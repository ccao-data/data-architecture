-- Copy of default.vw_pin_history that feeds the "Assessed Values" open data
-- asset.
-- Some columns from the feeder view may not be present in this view.

SELECT
    CONCAT(pin, year) AS row_id,
    pin,
    CAST(year AS INT) AS year,
    class,
    township_code,
    township_name,
    nbhd,
    mailed_bldg,
    mailed_land,
    mailed_tot,
    mailed_hie,
    certified_bldg,
    certified_land,
    certified_tot,
    certified_hie,
    board_bldg,
    board_land,
    board_tot,
    board_hie
FROM {{ ref('default.vw_pin_history') }}
