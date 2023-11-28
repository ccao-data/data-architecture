WITH sales_cte AS (
    SELECT
        substr(saledt, 1, 4) AS year,
        COUNT(CASE WHEN price IS NULL THEN 1 END) AS price_null_count
    FROM iasworld.sales
    WHERE CAST(substr(saledt, 1, 4) AS INTEGER) > 2014
    GROUP BY substr(saledt, 1, 4)
    ORDER BY substr(saledt, 1, 4)
)

SELECT
    year,
    price_null_count
FROM sales_cte
WHERE price_null_count > 1000
