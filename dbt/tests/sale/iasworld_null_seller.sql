SELECT
  substr(saledt, 1, 4) AS year,
    COUNT(CASE WHEN LENGTH(oldown) < 2 OR oldown IS NULL THEN 1 END) AS oldowner_null
FROM iasworld.sales
WHERE
  deactivat IS NULL
  AND cur = 'Y'
  AND sales.price IS NOT NULL
  AND sales.instruno IS NOT NULL
  AND CAST(substr(saledt, 1, 4) AS INTEGER) >= 2014
GROUP BY substr(saledt, 1, 4)
ORDER BY substr(saledt, 1, 4);