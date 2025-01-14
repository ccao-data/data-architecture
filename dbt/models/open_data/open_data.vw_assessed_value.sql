-- Copy of default.vw_pin_history that feeds the "Assessed Values" open data
-- asset.
-- Some columns from the feeder view may not be present in this view.

SELECT
    CONCAT(pin, year) AS row_id,
    pin,
    year,
    class,
    township_code,
    township_name,
    nbhd,
    mailed_bldg,
    mailed_land,
    mailed_tot,
    certified_bldg,
    certified_land,
    certified_tot,
    board_bldg,
    board_land,
    board_tot
FROM {{ ref('default.vw_pin_history') }}
