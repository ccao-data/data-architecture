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
    iasworld.year AS year_sales,
    iasworld.price,
    iasworld.instruno,
    mydec.year AS year_mydec,
    mydec.line_11_full_consideration,
    mydec.document_number
FROM sales_cte AS iasworld
FULL OUTER JOIN mydec_cte AS mydec
    ON iasworld.year = mydec.year
    AND iasworld.instruno = mydec.document_number
WHERE (iasworld.price != mydec.line_11_full_consideration);
