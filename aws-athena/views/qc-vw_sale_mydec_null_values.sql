-- View that identifies sales with null values in key fields.
SELECT
    SUBSTR(mydec.year_of_sale, 1, 4) AS year,
    CAST(
        COUNT(
            CASE WHEN mydec.line_11_full_consideration IS NULL THEN 1 END
        ) AS DOUBLE
    )
    / CAST(COUNT(*) AS DOUBLE) AS price,
    CAST(COUNT(CASE WHEN mydec.full_address IS NULL THEN 1 END) AS DOUBLE)
    / CAST(COUNT(*) AS DOUBLE) AS address,
    CAST(
        COUNT(
            CASE WHEN mydec.line_5_instrument_type IS NULL THEN 1 END
        ) AS DOUBLE
    )
    / CAST(COUNT(*) AS DOUBLE) AS deed_type,
    CAST(
        COUNT(
            CASE
                WHEN
                    LENGTH(mydec.buyer_name) < 2 OR mydec.buyer_name IS NULL
                    THEN 1
            END
        ) AS DOUBLE
    )
    / CAST(COUNT(*) AS DOUBLE) AS buyer,
    CAST(
        COUNT(
            CASE
                WHEN
                    LENGTH(mydec.seller_name) < 2 OR mydec.seller_name IS NULL
                    THEN 1
            END
        ) AS DOUBLE
    )
    / CAST(COUNT(*) AS DOUBLE) AS seller
FROM {{ source('sale', 'mydec') }} AS mydec
WHERE SUBSTR(mydec.year_of_sale, 1, 4) >= '2014'
GROUP BY SUBSTR(mydec.year_of_sale, 1, 4)
