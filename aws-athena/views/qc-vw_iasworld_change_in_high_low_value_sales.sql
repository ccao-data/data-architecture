WITH sales_cte AS (
    SELECT
        SUBSTR(sales.saledt, 1, 4) AS year,
        COUNT(CASE WHEN sales.price < 10000 THEN 1 END)
            AS price_less_than_10k_count,
        COUNT(CASE WHEN sales.price > 1000000 THEN 1 END)
            AS price_greater_than_1m_count
    FROM iasworld.sales
    WHERE
        sales.deactivat IS NULL
        AND sales.cur = 'Y'
        AND sales.price IS NOT NULL
        AND sales.instruno IS NOT NULL
        AND CAST(SUBSTR(sales.saledt, 1, 4) AS INTEGER) BETWEEN 2014 AND 2040
    GROUP BY SUBSTR(sales.saledt, 1, 4)
    ORDER BY SUBSTR(sales.saledt, 1, 4)
)

SELECT
    year,
    price_less_than_10k_count,
    price_greater_than_1m_count,
    prev_year_price_less_than_10k_count,
    prev_year_price_greater_than_1m_count
FROM (
    SELECT
        year,
        price_less_than_10k_count,
        price_greater_than_1m_count,
        LAG(price_less_than_10k_count)
            OVER (ORDER BY year)
            AS prev_year_price_less_than_10k_count,
        LAG(price_greater_than_1m_count)
            OVER (ORDER BY year)
            AS prev_year_price_greater_than_1m_count
    FROM sales_cte
) AS result
WHERE CAST(year AS INTEGER) > 2014;
