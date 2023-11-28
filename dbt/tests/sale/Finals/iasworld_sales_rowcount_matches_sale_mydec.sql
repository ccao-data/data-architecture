WITH sales_cte AS (
  SELECT
    substr(saledt, 1, 4) AS year,
    COUNT(DISTINCT instruno) AS IasWorld_sales
  FROM iasworld.sales
  WHERE
    deactivat IS NULL
    AND cur = 'Y'
    AND sales.instruno IS NOT NULL
    AND CAST(substr(saledt, 1, 4) AS INTEGER) >= 2014
  GROUP BY substr(saledt, 1, 4)
  ORDER BY substr(saledt, 1, 4)
),

mydec_cte AS (
  SELECT
    substr(year_of_sale, 1, 4) AS year,
    COUNT(DISTINCT document_number) AS my_dec_sales
  FROM sale.mydec
  WHERE CAST(substr(year_of_sale, 1, 4) AS INTEGER) >= 2014
  GROUP BY substr(year_of_sale, 1, 4)
  ORDER BY substr(year_of_sale, 1, 4)
),

comparison AS (
  SELECT
    s.year,
    s.IasWorld_sales,
    m.my_dec_sales,
    CASE
      WHEN s.IasWorld_sales > 1.05 * m.my_dec_sales THEN 'IasWorld 5% Higher'
      WHEN m.my_dec_sales > 1.05 * s.IasWorld_sales THEN 'Mydec 5% Higher'
      ELSE 'No significant difference'
    END AS comparison
  FROM sales_cte s
  JOIN mydec_cte m ON s.year = m.year
)

SELECT
  COUNT(*) AS failures,
  COUNT(*) != 0 AS should_warn,
  COUNT(*) != 0 AS should_error
FROM comparison
WHERE comparison = 'No significant difference'