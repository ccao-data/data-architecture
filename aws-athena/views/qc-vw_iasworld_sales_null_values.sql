WITH sales_cte AS (
    SELECT
        substr(saledt, 1, 4) AS year,
        COUNT(CASE WHEN price IS NULL THEN 1 END) AS price_null_count,
        COUNT(CASE WHEN instrtyp IS NULL THEN 1 END) AS deed_null_count,
        COUNT(CASE WHEN LENGTH(own1) < 2 OR own1 IS NULL THEN 1 END) AS buyer_null_count,
        COUNT(CASE WHEN LENGTH(oldown) < 2 OR oldown IS NULL THEN 1 END) AS seller_null_count
    FROM iasworld.sales
    WHERE
        deactivat IS NULL
        AND cur = 'Y'
        AND sales.instruno IS NOT NULL
        AND CAST(substr(saledt, 1, 4) AS INTEGER) >= 2014
    GROUP BY substr(saledt, 1, 4)
    ORDER BY substr(saledt, 1, 4)
)

SELECT
    year,
    price_null_count,
    deed_null_count,
    buyer_null_count,
    seller_null_count
FROM sales_cte