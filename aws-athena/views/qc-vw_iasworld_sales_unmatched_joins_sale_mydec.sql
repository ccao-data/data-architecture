-- View that counts the number of sales in iasWorld and MyDec that do not have
-- matching sales in the other dataset.
WITH sales_cte AS (
    SELECT DISTINCT
        SUBSTR(sales.saledt, 1, 4) AS taxyr,
        REGEXP_REPLACE(sales.instruno, '[^0-9]', '') AS instruno
    FROM {{ source('iasworld', 'sales') }} AS sales
    WHERE
        sales.deactivat IS NULL
        AND sales.cur = 'Y'
        AND sales.instruno IS NOT NULL
        AND SUBSTR(sales.saledt, 1, 4) >= '2014'
),

mydec_cte AS (
    SELECT DISTINCT
        SUBSTR(mydec.year_of_sale, 1, 4) AS taxyr,
        mydec.document_number
    FROM {{ source('sale', 'mydec') }} AS mydec
    WHERE SUBSTR(mydec.year_of_sale, 1, 4) >= '2014'
)

SELECT
    COALESCE(iasworld.taxyr, mydec.taxyr) AS taxyr,
    COUNT(iasworld.instruno) AS iasworld_unmatched,
    COUNT(mydec.document_number) AS mydec_unmatched
FROM sales_cte AS iasworld
FULL OUTER JOIN mydec_cte AS mydec
    ON iasworld.taxyr = mydec.taxyr
    AND iasworld.instruno = mydec.document_number
WHERE iasworld.instruno IS NULL OR mydec.document_number IS NULL
GROUP BY COALESCE(iasworld.taxyr, mydec.taxyr)
ORDER BY taxyr
