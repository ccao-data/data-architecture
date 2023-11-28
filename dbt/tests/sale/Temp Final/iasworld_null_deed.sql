WITH sales_cte AS (
    SELECT
        substr(saledt, 1, 4) AS year,
        COUNT(CASE WHEN instrtyp IS NULL THEN 1 END) AS deed_null_count
    FROM iasworld.sales
    WHERE CAST(substr(saledt, 1, 4) AS INTEGER) > 2014
    GROUP BY substr(saledt, 1, 4)
    ORDER BY substr(saledt, 1, 4)
)

SELECT
    year,
    deed_null_count
FROM sales_cte
WHERE deed_null_count > 1000