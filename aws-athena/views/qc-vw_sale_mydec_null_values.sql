-- View that identifies sales with null values in key fields.
SELECT
    SUBSTR(mydec.year_of_sale, 1, 4) AS year,
    mydec.line_11_full_consideration AS price,
    mydec.full_address AS address,
    mydec.line_5_instrument_type AS deed_type,
    CASE WHEN LENGTH(mydec.buyer_name) < 2 THEN NULL ELSE mydec.buyer_name END
        AS buyer,
    CASE WHEN LENGTH(mydec.seller_name) < 2 THEN NULL ELSE mydec.seller_name END
        AS seller
FROM {{ source('sale', 'mydec') }} AS mydec
WHERE SUBSTR(mydec.year_of_sale, 1, 4) >= '2014'
