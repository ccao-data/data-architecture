WITH sales_cte AS (
    SELECT
        SUBSTR(sales.saledt, 1, 4) AS year,
        COUNT(DISTINCT sales.instruno) AS iasworld_sales
    FROM iasworld.sales
    WHERE
        sales.deactivat IS NULL
        AND sales.cur = 'Y'
        AND sales.instruno IS NOT NULL
        AND CAST(SUBSTR(sales.saledt, 1, 4) AS INTEGER) >= 2014
    GROUP BY SUBSTR(sales.saledt, 1, 4)
    ORDER BY SUBSTR(sales.saledt, 1, 4)
),

mydec_cte AS (
    SELECT
        SUBSTR(year_of_sale, 1, 4) AS year,
        COUNT(DISTINCT document_number) AS my_dec_sales
    FROM sale.mydec
    WHERE CAST(SUBSTR(year_of_sale, 1, 4) AS INTEGER) >= 2014
    GROUP BY SUBSTR(year_of_sale, 1, 4)
    ORDER BY SUBSTR(year_of_sale, 1, 4)
),

comparison AS (
    SELECT
        s.year,
        s.iasworld_sales,
        m.my_dec_sales,
        CASE
            WHEN
                s.iasworld_sales > 1.05 * m.my_dec_sales
                THEN 'IasWorld 5% Higher'
            WHEN m.my_dec_sales > 1.05 * s.iasworld_sales THEN 'Mydec 5% Higher'
            ELSE 'No significant difference'
        END AS comparison
    FROM sales_cte AS s
    INNER JOIN mydec_cte AS m ON s.year = m.year
)

SELECT * FROM comparison
