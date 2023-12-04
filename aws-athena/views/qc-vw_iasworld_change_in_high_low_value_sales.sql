WITH sales_cte AS (
  SELECT
    substr(saledt, 1, 4) AS year,
    COUNT(CASE WHEN sales.price < 10000 THEN 1 END) AS price_less_than_10k_count,
    COUNT(CASE WHEN sales.price > 1000000 THEN 1 END) AS price_greater_than_1m_count
  FROM iasworld.sales
  WHERE
    deactivat IS NULL
    AND cur = 'Y'
    AND sales.price IS NOT NULL
    AND sales.instruno IS NOT NULL
    AND CAST(substr(saledt, 1, 4) AS INTEGER) >= 2014
    AND CAST(substr(saledt, 1, 4) AS INTEGER) <> 2500 
  GROUP BY substr(saledt, 1, 4)
  ORDER BY substr(saledt, 1, 4)
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
    LAG(price_less_than_10k_count) OVER (ORDER BY year) AS prev_year_price_less_than_10k_count,
    LAG(price_greater_than_1m_count) OVER (ORDER BY year) AS prev_year_price_greater_than_1m_count
  FROM sales_cte
) result
WHERE CAST(year AS INTEGER) > 2014;
