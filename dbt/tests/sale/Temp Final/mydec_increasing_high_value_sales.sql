WITH mydec_cte AS (
  SELECT
    substr(year_of_sale, 1, 4) AS year,
    COUNT(CASE WHEN line_11_full_consideration >1000000 THEN 1 END) AS price_greater_than_1million,
    MAX(line_11_full_consideration) AS max_price
  FROM sale.mydec
  WHERE CAST(substr(year_of_sale, 1, 4) AS INTEGER) >= 2014
  GROUP BY substr(year_of_sale, 1, 4)
  ORDER BY substr(year_of_sale, 1, 4)
)

SELECT
  year,
  price_greater_than_1million,
  LAG(price_greater_than_1million) OVER (ORDER BY year) AS price_greater_than_1million,
  CASE
    WHEN LAG(price_greater_than_1million) OVER (ORDER BY year) IS NOT NULL AND price_greater_than_1million > 1.05 * LAG(price_greater_than_1million) OVER (ORDER BY year) THEN 'More than 5% growth'
    WHEN LAG(price_greater_than_1million) OVER (ORDER BY year) IS NOT NULL AND price_greater_than_1million < .95 * LAG(price_greater_than_1million) OVER (ORDER BY year) THEN 'More than 5 % decrease'
    ELSE 'No significant change'
  END AS growth_status
FROM mydec_cte