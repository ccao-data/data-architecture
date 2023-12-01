WITH mydec_cte AS (
  SELECT
    CAST(substr(year_of_sale, 1, 4) AS INTEGER) AS year,
    COUNT(CASE WHEN line_11_full_consideration < 10000 THEN 1 END) AS price_less_than_10k_count,
    COUNT(CASE WHEN line_11_full_consideration > 1000000 THEN 1 END) AS price_greater_than_1m_count
  FROM sale.mydec
  WHERE CAST(substr(year_of_sale, 1, 4) AS INTEGER) >= 2014
  GROUP BY CAST(substr(year_of_sale, 1, 4) AS INTEGER)
  ORDER BY CAST(substr(year_of_sale, 1, 4) AS INTEGER)
)

SELECT
  year,
  price_less_than_10k_count,
  price_greater_than_1m_count,
  prev_year_price_less_than_10k_count,
  prev_year_price_greater_than_1m_count,
  CASE
    WHEN prev_year_price_less_than_10k_count IS NOT NULL AND price_less_than_10k_count > 1.05 * prev_year_price_less_than_10k_count THEN 'More than 5% growth'
    WHEN prev_year_price_less_than_10k_count IS NOT NULL AND price_less_than_10k_count < .95 * prev_year_price_less_than_10k_count THEN 'More than 5% decrease'
    ELSE 'No significant change'
  END AS growth_status
FROM (
  SELECT
    year,
    price_less_than_10k_count,
    price_greater_than_1m_count,
    LAG(price_less_than_10k_count) OVER (ORDER BY year) AS prev_year_price_less_than_10k_count,
    LAG(price_greater_than_1m_count) OVER (ORDER BY year) AS prev_year_price_greater_than_1m_count
  FROM mydec_cte
) result
WHERE year > 2014;
