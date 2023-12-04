WITH mydec_sales AS (
    SELECT
        SUBSTR(year_of_sale, 1, 4) AS year,
        COUNT(CASE WHEN line_11_full_consideration IS NULL THEN 1 END)
            AS price_null_count,
        COUNT(CASE WHEN full_address IS NULL THEN 1 END) AS address_null_count,
        COUNT(CASE WHEN line_5_instrument_type IS NULL THEN 1 END)
            AS deed_null_count,
        COUNT(
            CASE WHEN LENGTH(buyer_name) < 2 OR buyer_name IS NULL THEN 1 END
        ) AS buyer_null_count,
        COUNT(
            CASE WHEN LENGTH(seller_name) < 2 OR seller_name IS NULL THEN 1 END
        ) AS seller_null_count
    FROM sale.mydec
    WHERE CAST(SUBSTR(year_of_sale, 1, 4) AS INTEGER) >= 2014
    GROUP BY SUBSTR(year_of_sale, 1, 4)
    ORDER BY SUBSTR(year_of_sale, 1, 4)
)

SELECT
    year,
    price_null_count,
    address_null_count,
    deed_null_count,
    buyer_null_count,
    seller_null_count
FROM mydec_sales
