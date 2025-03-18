-- Copy of default.vw_pin_sale that feeds the "Parcel Sales" open data asset.
-- Some columns from the feeder view may not be present in this view.

SELECT
    CAST(sale_key AS VARCHAR) AS row_id,
    pin,
    year,
    township_code,
    nbhd,
    class,
    sale_date,
    is_mydec_date,
    sale_price,
    doc_no,
    CASE
        WHEN deed_type = '01' THEN 'Warranty'
        WHEN deed_type = '02' THEN 'Trustee'
        WHEN deed_type = '03' THEN 'Quit claim'
        WHEN deed_type = '04' THEN 'Executor'
        WHEN deed_type = '05' THEN 'Other'
        WHEN deed_type = '06' THEN 'Beneficiary'
        WHEN deed_type = '99' THEN 'Unknown'
    END AS deed_type,
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
