WITH sales_cte AS (
  SELECT
    substr(saledt, 1, 4) AS year,
    REGEXP_REPLACE(instruno, '[^0-9]', '') AS instruno
  FROM iasworld.sales
  WHERE
    deactivat IS NULL
    AND cur = 'Y'
    AND substr(saledt, 1, 4) >= '2014'
    AND CAST(substr(saledt, 1, 4) AS INTEGER) >= 2014
  GROUP BY substr(saledt, 1, 4), instruno
),

mydec_cte AS (
  SELECT
    substr(year_of_sale, 1, 4) AS year,
    document_number
  FROM sale.mydec
  WHERE CAST(substr(year_of_sale, 1, 4) AS INTEGER) >= 2014
  GROUP BY substr(year_of_sale, 1, 4), document_number
)

SELECT
  COALESCE(s.year, m.year) AS year,
  COUNT(s.instruno) AS IasWorld_unmatched
FROM sales_cte s
LEFT JOIN mydec_cte m ON s.year = m.year AND s.instruno = m.document_number
WHERE s.instruno IS NULL OR m.document_number IS NULL
GROUP BY COALESCE(s.year, m.year)
ORDER BY year
