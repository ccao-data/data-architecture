WITH mydec_cte AS (
  SELECT
    substr(year_of_sale, 1, 4) AS year,
    COUNT(CASE WHEN line_5_instrument_type IS NULL THEN 1 END) AS deed_null_count  
  FROM sale.mydec
  WHERE CAST(substr(year_of_sale, 1, 4) AS INTEGER) >= 2014
  GROUP BY substr(year_of_sale, 1, 4)
  ORDER BY substr(year_of_sale, 1, 4)
)
SELECT
  year,
  deed_null_count
FROM mydec_cte
WHERE deed_null_count > 1000