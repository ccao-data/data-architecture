WITH mydec_cte AS (
  SELECT
    substr(year_of_sale, 1, 4) AS year,
    COUNT(CASE WHEN line_11_full_consideration < 10000 THEN 1 END) AS price_less_than_10k_count,
    MAX(line_11_full_consideration) AS max_price
  FROM sale.mydec
  WHERE CAST(substr(year_of_sale, 1, 4) AS INTEGER) >= 2014
  GROUP BY substr(year_of_sale, 1, 4)
  ORDER BY substr(year_of_sale, 1, 4)
)

SELECT
  year,
  price_less_than_10k_count,
  LAG(price_less_than_10k_count) OVER (ORDER BY year) AS price_less_than_10k_count,
  CASE
    WHEN LAG(price_less_than_10k_count) OVER (ORDER BY year) IS NOT NULL AND price_less_than_10k_count > 1.05 * LAG(price_less_than_10k_count) OVER (ORDER BY year) THEN 'More than 5% growth'
    WHEN LAG(price_less_than_10k_count) OVER (ORDER BY year) IS NOT NULL AND price_less_than_10k_count < .95 * LAG(price_less_than_10k_count) OVER (ORDER BY year) THEN 'More than 5 % decrease'
    ELSE 'No significant change'
  END AS growth_status
FROM mydec_cte