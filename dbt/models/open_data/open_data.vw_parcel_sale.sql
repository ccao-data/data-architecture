-- Copy of default.vw_pin_sale that feeds the "Parcel Sales" open data asset.
-- Some columns from the feeder view may not be present in this view.

SELECT
    CAST(vps.sale_key AS VARCHAR) AS row_id,
    vps.pin,
    CAST(vps.year AS INT) AS year,
    vps.township_code,
    vps.nbhd,
    vps.class,
    vps.sale_date,
    vps.is_mydec_date,
    vps.sale_price,
    vps.doc_no,
    deeds.deed_name AS deed_type,
    vps.seller_name,
    vps.is_multisale,
    vps.num_parcels_sale,
    vps.buyer_name,
    vps.sale_type,
    vps.sale_filter_same_sale_within_365,
    vps.sale_filter_less_than_10k,
    vps.sale_filter_deed_type,
    vps.mydec_deed_type
FROM {{ ref('default.vw_pin_sale') }} AS vps
-- Join to get deed type description
LEFT JOIN {{ ref('sale.deed_type') }} AS deeds
    ON vps.deed_type = deeds.deed_num
