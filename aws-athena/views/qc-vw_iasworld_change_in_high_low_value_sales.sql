-- View that counts the number of iasWorld sales with a price less than 10k or
-- greater than 1m for each year. Includes the previous year's count for
-- comparison.
WITH sales_cte AS (
    SELECT
        SUBSTR(sales.saledt, 1, 4) AS year,
        COUNT(CASE WHEN sales.price < 10000 THEN 1 END)
            AS price_less_than_10k_count,
        COUNT(CASE WHEN sales.price > 1000000 THEN 1 END)
            AS price_greater_than_1m_count,
        LAG(
            COUNT(CASE WHEN sales.price < 10000 THEN 1 END)
        ) OVER (ORDER BY SUBSTR(sales.saledt, 1, 4))
            AS prev_year_price_less_than_10k_count,
        LAG(
            COUNT(
                CASE WHEN sales.price > 1000000 THEN 1 END
            )
        ) OVER (ORDER BY SUBSTR(sales.saledt, 1, 4))
            AS prev_year_price_greater_than_1m_count
    FROM {{ source('iasworld', 'sales') }} AS sales
    WHERE
        sales.deactivat IS NULL
        AND sales.cur = 'Y'
        AND sales.price IS NOT NULL
        AND sales.instruno IS NOT NULL
        AND CAST(SUBSTR(sales.saledt, 1, 4) AS INTEGER) BETWEEN 2014 AND YEAR(
            CURRENT_DATE
        )
    GROUP BY SUBSTR(sales.saledt, 1, 4)
)

SELECT
    year,
    prev_year_price_less_than_10k_count,
    price_less_than_10k_count,
    CASE
        WHEN prev_year_price_less_than_10k_count IS NOT NULL
            AND price_less_than_10k_count
            > 2 * prev_year_price_less_than_10k_count
            THEN 'More than 200% growth'
        WHEN prev_year_price_less_than_10k_count IS NOT NULL
            AND price_less_than_10k_count
            < .5 * prev_year_price_less_than_10k_count
            THEN 'More than 50% decrease'
        ELSE 'No significant change'
    END AS price_less_than_10k_growth_status,
    prev_year_price_greater_than_1m_count,
    price_greater_than_1m_count,
    CASE
        WHEN prev_year_price_greater_than_1m_count IS NOT NULL
            AND price_greater_than_1m_count
            > 1.1 * prev_year_price_greater_than_1m_count
            THEN 'More than 10% growth'
        WHEN prev_year_price_greater_than_1m_count IS NOT NULL
            AND price_greater_than_1m_count
            < .9 * prev_year_price_greater_than_1m_count
            THEN 'More than 1% decrease'
        ELSE 'No significant change'
    END AS price_greater_than_1m_growth_status
FROM sales_cte
WHERE year > '2014'
ORDER BY year ASC
