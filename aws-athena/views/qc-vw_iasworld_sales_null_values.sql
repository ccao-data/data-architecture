WITH sales_cte AS (
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
        )
            AS seller_null_count
    FROM iasworld.sales
    WHERE
        sales.deactivat IS NULL
        AND sales.cur = 'Y'
        AND sales.instruno IS NOT NULL
        AND CAST(SUBSTR(sales.saledt, 1, 4) AS INTEGER) >= 2014
    GROUP BY SUBSTR(sales.saledt, 1, 4)
    ORDER BY SUBSTR(sales.saledt, 1, 4)
)

SELECT
    year,
    price_null_count,
    deed_null_count,
    buyer_null_count,
    seller_null_count
FROM sales_cte
