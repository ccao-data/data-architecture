WITH mydec_cte AS (
  SELECT
    substr(year_of_sale, 1, 4) AS year,
    COUNT(CASE WHEN line_11_full_consideration IS NULL THEN 1 END) AS price_null_count  
  FROM sale.mydec
  WHERE CAST(substr(year_of_sale, 1, 4) AS INTEGER) >= 2014
  GROUP BY substr(year_of_sale, 1, 4)
  ORDER BY substr(year_of_sale, 1, 4)
)
SELECT
  year,
  price_null_count
FROM mydec_cte
WHERE price_null_count > 1000