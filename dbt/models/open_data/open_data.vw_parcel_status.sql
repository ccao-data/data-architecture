-- Copy of defualt.vw_pin_status that feeds the "Parcel Status" open data asset.

/* The following columns are not included in the open data asset, or are
currently hidden:
    ward_name
*/

SELECT
    CONCAT(pin, year) AS row_id,
    pin,
    year,
    class,
    is_corner_lot,
    is_ahsap,
    is_exempt,
    is_zero_bill,
    is_parking_space,
    parking_space_flag_reason,
    is_common_area,
    is_leasehold,
    is_mixed_use,
    is_railroad,
    is_weird,
    weird_flag_reason,
    oby_cdu_code,
    oby_cdu_description,
    comdat_cdu_code,
    comdat_cdu_description,
    dweldat_cdu_code,
    dweldat_cdu_description,
    pardat_note,
    is_filler_class,
    is_filler_pin
FROM {{ ref('default.vw_pin_status') }}
