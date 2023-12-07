-- View that identifies sales with null values in key fields.
SELECT
    SUBSTR(sales.saledt, 1, 4) AS year,
    COUNT(CASE WHEN sales.price IS NULL THEN 1 END) AS price_null_count,
    COUNT(CASE WHEN sales.instrtyp IS NULL THEN 1 END) AS deed_null_count,
    COUNT(CASE WHEN LENGTH(sales.own1) < 2 OR sales.own1 IS NULL THEN 1 END)
        AS buyer_null_count,
    COUNT(
        CASE
            WHEN LENGTH(sales.oldown) < 2 OR sales.oldown IS NULL THEN 1
        END
    ) AS seller_null_count
FROM {{ source('iasworld', 'sales') }} AS sales
WHERE
    sales.deactivat IS NULL
    AND sales.cur = 'Y'
    AND sales.instruno IS NOT NULL
    AND CAST(SUBSTR(sales.saledt, 1, 4) AS INTEGER) BETWEEN 2014 AND YEAR(
        CURRENT_DATE
    )
GROUP BY SUBSTR(sales.saledt, 1, 4)
ORDER BY SUBSTR(sales.saledt, 1, 4)
