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
    AND CAST(substr(saledt, 1, 4) AS INTEGER) <> 2500 
  GROUP BY substr(saledt, 1, 4)
  ORDER BY substr(saledt, 1, 4)
),

filtered_sales AS (
  SELECT
    year,
    sales_table_n,
    LAG(sales_table_n) OVER (ORDER BY year) AS prev_year_sales
  FROM sales_cte
  WHERE sales_table_n IS NOT NULL
)

SELECT
  year,
  sales_table_n,
  prev_year_sales,
  CASE
    WHEN prev_year_sales IS NOT NULL AND sales_table_n > 1.25 * prev_year_sales THEN 'More than 25% growth'
    WHEN prev_year_sales IS NOT NULL AND sales_table_n < 0.75 * prev_year_sales THEN 'More than 25% decrease'
    ELSE 'No significant change'
  END AS growth_status
FROM filtered_sales;
