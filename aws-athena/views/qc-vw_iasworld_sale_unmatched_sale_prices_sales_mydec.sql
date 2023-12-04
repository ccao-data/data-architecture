WITH sales_cte AS (
    SELECT
        SUBSTR(saledt, 1, 4) AS year,
        price,
        REGEXP_REPLACE(instruno, '[^0-9]', '') AS instruno
    FROM iasworld.sales
    WHERE
        deactivat IS NULL
        AND cur = 'Y'
        AND SUBSTR(saledt, 1, 4) >= '2014'
        AND CAST(SUBSTR(saledt, 1, 4) AS INTEGER) >= 2014
),

mydec_cte AS (
    SELECT
        SUBSTR(year_of_sale, 1, 4) AS year,
        line_11_full_consideration,
        document_number
    FROM sale.mydec
    WHERE CAST(SUBSTR(year_of_sale, 1, 4) AS INTEGER) >= 2014
)

SELECT
    s.year AS year_sales,
    s.price,
    s.instruno,
    m.year AS year_mydec,
    m.line_11_full_consideration,
    m.document_number
FROM sales_cte AS s
FULL OUTER JOIN mydec_cte AS m
    ON s.year = m.year
    AND s.instruno = m.document_number
WHERE (s.price != m.line_11_full_consideration);
