-- View that compares the number of unique sales between iasWorld and myDec.
WITH sales_cte AS (
    SELECT
        SUBSTR(sales.saledt, 1, 4) AS year,
        COUNT(DISTINCT sales.instruno) AS iasworld_sales
    FROM {{ source('iasworld', 'sales') }} AS sales
    WHERE
        sales.deactivat IS NULL
        AND sales.cur = 'Y'
        AND sales.instruno IS NOT NULL
        AND CAST(
            SUBSTR(sales.saledt, 1, 4) AS INTEGER
        ) BETWEEN 2014 AND 2040
    GROUP BY SUBSTR(sales.saledt, 1, 4)
),

mydec_cte AS (
    SELECT
        SUBSTR(mydec.year_of_sale, 1, 4) AS year,
        COUNT(DISTINCT mydec.document_number) AS my_dec_sales
    FROM {{ source('sale', 'mydec') }} AS mydec
    WHERE SUBSTR(mydec.year_of_sale, 1, 4) >= '2014'
    GROUP BY SUBSTR(mydec.year_of_sale, 1, 4)
)

SELECT
    iasworld.year AS comparison_year,
    iasworld.iasworld_sales,
    mydec.my_dec_sales,
    CASE
        WHEN
            iasworld.sales > 1.05 * mydec.my_dec_sales
            THEN 'IasWorld 5% Higher'
        WHEN
            mydec.my_dec_sales > 1.05 * iasworld.sales
            THEN 'Mydec 5% Higher'
        ELSE 'No significant difference'
    END AS comparison
FROM sales_cte AS iasworld
INNER JOIN mydec_cte AS mydec ON iasworld.year = mydec.year
ORDER BY iasworld.year ASC
