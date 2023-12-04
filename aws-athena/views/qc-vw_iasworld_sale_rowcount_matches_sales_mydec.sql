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
        SUBSTR(mydec.year_of_sale, 1, 4) AS year,
        COUNT(DISTINCT mydec.document_number) AS my_dec_sales
    FROM sale.mydec AS mydec
    WHERE CAST(SUBSTR(mydec.year_of_sale, 1, 4) AS INTEGER) >= 2014
    GROUP BY SUBSTR(mydec.year_of_sale, 1, 4)
    ORDER BY SUBSTR(mydec.year_of_sale, 1, 4)
),

comparison AS (
    SELECT
        iasworld.year AS comparison_year,
        iasworld.iasworld_sales,
        mydec.my_dec_sales,
        CASE
            WHEN
                iasworld.iasworld_sales > 1.05 * mydec.my_dec_sales
                THEN 'IasWorld 5% Higher'
            WHEN
                mydec.my_dec_sales > 1.05 * iasworld.iasworld_sales
                THEN 'Mydec 5% Higher'
            ELSE 'No significant difference'
        END AS comparison
    FROM iasworld_sales_cte AS iasworld
    INNER JOIN mydec_cte AS mydec ON iasworld.year = mydec.year
)

SELECT * FROM comparison;
