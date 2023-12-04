SELECT
    SUBSTR(sales.saledt, 1, 4) AS year,
    SUBSTR(sales.saledt, 9, 2) AS day_of_month,
    SUM(COUNT(DISTINCT sales.instruno))
        OVER (PARTITION BY SUBSTR(sales.saledt, 1, 4))
        AS total_sales,
    COUNT(DISTINCT sales.instruno) AS sales_by_day
FROM iasworld.sales
WHERE
    sales.deactivat IS NULL
    AND sales.cur = 'Y'
    AND sales.price IS NOT NULL
    AND sales.instruno IS NOT NULL
    AND CAST(SUBSTR(sales.saledt, 1, 4) AS INTEGER) >= 2014
GROUP BY SUBSTR(sales.saledt, 1, 4), SUBSTR(sales.saledt, 9, 2)
ORDER BY SUBSTR(sales.saledt, 1, 4), SUBSTR(sales.saledt, 9, 2);
