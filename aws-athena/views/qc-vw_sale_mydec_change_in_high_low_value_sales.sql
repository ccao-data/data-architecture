-- View that counts the number of MyDec sales with a price less than 10k or
-- greater than 1m for each year. Includes the previous year's count for
-- comparison.
WITH sales_cte AS (
    SELECT
        mydec.year_of_sale AS year,
        COUNT(CASE WHEN mydec.line_11_full_consideration < 10000 THEN 1 END)
            AS price_less_than_10k_count,
        COUNT(CASE WHEN mydec.line_11_full_consideration > 1000000 THEN 1 END)
            AS price_greater_than_1m_count,
        LAG(
            COUNT(CASE WHEN mydec.line_11_full_consideration < 10000 THEN 1 END)
        ) OVER (ORDER BY mydec.year_of_sale)
            AS prev_year_price_less_than_10k_count,
        LAG(
            COUNT(
                CASE WHEN mydec.line_11_full_consideration > 1000000 THEN 1 END
            )
        ) OVER (ORDER BY mydec.year_of_sale)
            AS prev_year_price_greater_than_1m_count
    FROM {{ source('sale', 'mydec') }} AS mydec
    WHERE mydec.year_of_sale >= '2014'
    GROUP BY mydec.year_of_sale
)

SELECT
    year,
    prev_year_price_less_than_10k_count,
    price_less_than_10k_count,
    CASE
        WHEN prev_year_price_less_than_10k_count IS NOT NULL
            AND price_less_than_10k_count
            > 1.05 * prev_year_price_less_than_10k_count
            THEN 'More than 5% growth'
        WHEN prev_year_price_less_than_10k_count IS NOT NULL
            AND price_less_than_10k_count
            < .95 * prev_year_price_less_than_10k_count
            THEN 'More than 5% decrease'
        ELSE 'No significant change'
    END AS price_less_than_10k_growth_status,
    prev_year_price_greater_than_1m_count,
    price_greater_than_1m_count,
    CASE
        WHEN prev_year_price_greater_than_1m_count IS NOT NULL
            AND price_greater_than_1m_count
            > 1.05 * prev_year_price_greater_than_1m_count
            THEN 'More than 5% growth'
        WHEN prev_year_price_greater_than_1m_count IS NOT NULL
            AND price_greater_than_1m_count
            < .95 * prev_year_price_greater_than_1m_count
            THEN 'More than 5% decrease'
        ELSE 'No significant change'
    END AS price_greater_than_1m_growth_status
FROM sales_cte
WHERE year > '2014'
ORDER BY year ASC
