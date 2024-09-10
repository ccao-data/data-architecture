WITH mydec_sales as (
    SELECT
        REPLACE(line_1_primary_pin, '-', '') AS pin,
        REPLACE(document_number, 'D', '') AS doc_no,
        DATE_PARSE(line_4_instrument_date, '%Y-%m-%d') AS sale_date,
        line_11_full_consideration as sale_price,
        NULLIF(TRIM(seller_name), '') AS seller_name,
        NULLIF(TRIM(buyer_name), '') AS buyer_name
    FROM sale.mydec
)