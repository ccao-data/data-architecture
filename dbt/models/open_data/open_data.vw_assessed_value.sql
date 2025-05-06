-- Copy of default.vw_pin_history that feeds the "Assessed Values" open data
-- asset.
-- Some columns from the feeder view may not be present in this view.

WITH feeder AS (
    SELECT
        pin,
        CAST(year AS INT) AS feeder_year,
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
),

{{ open_data_rows_to_delete(feeder) }}
