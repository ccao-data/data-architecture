WITH iasworld_sales_cte AS (
    SELECT
        SUBSTR(iasworld.sales.saledt, 1, 4) AS year,
        COUNT(DISTINCT iasworld.sales.instruno) AS iasworld_sales
    FROM iasworld.sales
    WHERE
        iasworld.sales.deactivat IS NULL
        AND iasworld.sales.cur = 'Y'
        AND iasworld.sales.instruno IS NOT NULL
        AND CAST(SUBSTR(iasworld.sales.saledt, 1, 4) AS INTEGER) >= 2014
    GROUP BY SUBSTR(iasworld.sales.saledt, 1, 4)
    ORDER BY SUBSTR(iasworld.sales.saledt, 1, 4)
),

mydec_cte AS (
    SELECT
        SUBSTR(m.year_of_sale, 1, 4) AS year,
        COUNT(DISTINCT m.document_number) AS my_dec_sales
    FROM sale.mydec AS m
    WHERE CAST(SUBSTR(m.year_of_sale, 1, 4) AS INTEGER) >= 2014
    GROUP BY SUBSTR(m.year_of_sale, 1, 4)
    ORDER BY SUBSTR(m.year_of_sale, 1, 4)
),

comparison AS (
    SELECT
        i.year AS comparison_year,
        i.iasworld_sales,
        m.my_dec_sales,
        CASE
            WHEN
                i.iasworld_sales > 1.05 * m.my_dec_sales
                THEN 'IasWorld 5% Higher'
            WHEN m.my_dec_sales > 1.05 * i.iasworld_sales THEN 'Mydec 5% Higher'
            ELSE 'No significant difference'
        END AS comparison
    FROM iasworld_sales_cte AS i
    INNER JOIN mydec_cte AS m ON i.year = m.year
)

SELECT * FROM comparison;
