-- Copy of default.vw_pin_history that feeds the "Assessed Values" open data
-- asset.

/* The following columns are not included in the open data asset, or are
currently hidden:
    mailed_class
    certified_class
    board_class
    change_reason
    oneyr_pri_mailed_bldg
    oneyr_pri_mailed_land
    oneyr_pri_mailed_tot
    oneyr_pri_certified_bldg
    oneyr_pri_certified_land
    oneyr_pri_certified_tot
    oneyr_pri_board_bldg
    oneyr_pri_board_land
    oneyr_pri_board_tot
    oneyr_pri_change_reason
    twoyr_pri_mailed_bldg
    twoyr_pri_mailed_land
    twoyr_pri_mailed_tot
    twoyr_pri_certified_bldg
    twoyr_pri_certified_land
    twoyr_pri_certified_tot
    twoyr_pri_board_bldg
    twoyr_pri_board_land
    twoyr_pri_board_tot
    twoyr_pri_change_reason
*/

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
