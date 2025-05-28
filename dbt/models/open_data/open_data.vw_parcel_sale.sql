-- Copy of default.vw_pin_sale that feeds the "Parcel Sales" open data asset.
-- Some columns from the feeder view may not be present in this view.

SELECT
    feeder.pin,
    feeder.township_code,
    feeder.nbhd,
    feeder.class,
    feeder.sale_date,
    feeder.is_mydec_date,
    feeder.sale_price,
    feeder.doc_no,
    CASE
        WHEN feeder.deed_type = '01' THEN 'Warranty'
        WHEN feeder.deed_type = '02' THEN 'Trustee'
        WHEN feeder.deed_type = '03' THEN 'Quit claim'
        WHEN feeder.deed_type = '04' THEN 'Executor'
        WHEN feeder.deed_type = '05' THEN 'Other'
        WHEN feeder.deed_type = '06' THEN 'Beneficiary'
        WHEN feeder.deed_type = '99' THEN 'Unknown'
    END AS deed_type,
    feeder.seller_name,
    feeder.is_multisale,
    feeder.num_parcels_sale,
    feeder.buyer_name,
    feeder.sale_type,
    feeder.sale_filter_same_sale_within_365,
    feeder.sale_filter_less_than_10k,
    feeder.sale_filter_deed_type,
    feeder.mydec_deed_type,
    {{ open_data_columns(row_id_cols=['sale_key']) }}
FROM {{ ref('default.vw_pin_sale') }} AS feeder
FULL OUTER JOIN
    (

        SELECT
            salekey AS row_id,
            SUBSTR(saledt, 1, 4) AS year,
            TRUE AS ":deleted" -- noqa: RF05
        FROM {{ source("iasworld", "sales") }}
        WHERE deactivat IS NOT NULL

    ) AS deleted_rows
    ON feeder.sale_key
    = deleted_rows.row_id
