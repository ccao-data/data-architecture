-- Copy of default.vw_pin_sale that feeds the "Parcel Sales" open data asset.
-- Some columns from the feeder view may not be present in this view.

SELECT
    sale_key AS row_id,
    pin,
    year,
    township_code,
    nbhd,
    class,
    sale_date,
    is_mydec_date,
    sale_price,
    doc_no,
    deed_type,
    seller_name,
    is_multisale,
    num_parcels_sale,
    buyer_name,
    sale_type,
    sale_filter_same_sale_within_365,
    sale_filter_less_than_10k,
    sale_filter_deed_type,
    mydec_deed_type
FROM {{ ref('default.vw_pin_sale') }}
