SELECT
  substr(saledt, 1, 4) AS year,
  substr(saledt, 9, 2) AS day_of_month,
  SUM(COUNT(DISTINCT instruno)) OVER (PARTITION BY substr(saledt, 1, 4)) AS total_sales,
  COUNT(DISTINCT instruno) AS sales_by_day
FROM iasworld.sales
WHERE
  deactivat IS NULL
  AND cur = 'Y'
  AND sales.price IS NOT NULL
  AND sales.instruno IS NOT NULL
  AND CAST(substr(saledt, 1, 4) AS INTEGER) >= 2014
GROUP BY substr(saledt, 1, 4), substr(saledt, 9, 2)
ORDER BY substr(saledt, 1, 4), substr(saledt, 9, 2);
