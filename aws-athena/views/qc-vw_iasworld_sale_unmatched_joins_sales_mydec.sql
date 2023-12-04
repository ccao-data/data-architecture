WITH sales_cte AS (
    SELECT
        SUBSTR(saledt, 1, 4) AS year,
        REGEXP_REPLACE(instruno, '[^0-9]', '') AS instruno
    FROM iasworld.sales
    WHERE
        deactivat IS NULL
        AND cur = 'Y'
        AND SUBSTR(saledt, 1, 4) >= '2014'
        AND CAST(SUBSTR(saledt, 1, 4) AS INTEGER) >= 2014
    GROUP BY SUBSTR(saledt, 1, 4), instruno
),

mydec_cte AS (
    SELECT
        SUBSTR(year_of_sale, 1, 4) AS year,
        document_number
    FROM sale.mydec
    WHERE CAST(SUBSTR(year_of_sale, 1, 4) AS INTEGER) >= 2014
    GROUP BY SUBSTR(year_of_sale, 1, 4), document_number
)

SELECT
    COALESCE(s.year, m.year) AS year,
    COUNT(s.instruno) AS iasworld_unmatched,
    COUNT(m.document_number) AS my_dec_unmatched
FROM sales_cte AS iasworld
FULL OUTER JOIN
    mydec_cte AS mydec
    ON iasworld.year = m.year AND iasworld.instruno = mydec.document_number
WHERE iasworld.instruno IS NULL OR mydec.document_number IS NULL
GROUP BY COALESCE(iasworld.year, mydec.year)
ORDER BY year
