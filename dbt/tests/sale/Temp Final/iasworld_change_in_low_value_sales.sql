WITH sales_cte AS (
  SELECT
    substr(saledt, 1, 4) AS year,
    COUNT(DISTINCT instruno) AS sales_table_n
  FROM iasworld.sales
  WHERE
    deactivat IS NULL
    AND cur = 'Y'
    AND sales.price IS NOT NULL
    AND sales.price < 10000
    AND sales.instruno IS NOT NULL
    AND CAST(substr(saledt, 1, 4) AS INTEGER) >= 2014
  GROUP BY substr(saledt, 1, 4)
  ORDER BY substr(saledt, 1, 4)
)

SELECT
  year,
  sales_table_n,
  LAG(sales_table_n) OVER (ORDER BY year) AS prev_year_sales,
  CASE
    WHEN LAG(sales_table_n) OVER (ORDER BY year) IS NOT NULL AND sales_table_n > 1.05 * LAG(sales_table_n) OVER (ORDER BY year) THEN 'More than 5% growth'
    WHEN LAG(sales_table_n) OVER (ORDER BY year) IS NOT NULL AND sales_table_n < .95 * LAG(sales_table_n) OVER (ORDER BY year) THEN 'More than 5 % decrease'
    ELSE 'No significant change'
  END AS growth_status
FROM sales_cte