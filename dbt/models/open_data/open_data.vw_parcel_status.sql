-- Copy of defualt.vw_pin_status that feeds the "Parcel Status" open data asset.
-- Some columns from the feeder view may not be present in this view.

SELECT
    feeder.pin,
    feeder.class,
    feeder.is_corner_lot,
    feeder.is_ahsap,
    feeder.is_exempt,
    feeder.is_zero_bill,
    feeder.is_parking_space,
    feeder.parking_space_flag_reason,
    feeder.is_common_area,
    feeder.is_leasehold,
    feeder.is_mixed_use,
    feeder.is_railroad,
    feeder.is_weird,
    feeder.weird_flag_reason,
    feeder.oby_cdu_code,
    feeder.oby_cdu_description,
    feeder.comdat_cdu_code,
    feeder.comdat_cdu_description,
    feeder.dweldat_cdu_code,
    feeder.dweldat_cdu_description,
    feeder.pardat_note,
    feeder.is_filler_class,
    feeder.is_filler_pin,
    {{ open_data_columns() }}
FROM {{ ref('default.vw_pin_status') }} AS feeder
{{ open_data_rows_to_delete(allow_999=true) }}
