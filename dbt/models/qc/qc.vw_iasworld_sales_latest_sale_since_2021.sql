SELECT
    parid,
    instruno,
    price,
    DATE_FORMAT(
        DATE_PARSE(saledt, '%Y-%m-%d %H:%i:%S.%f'), '%c/%e/%Y'
    ) AS saledt
FROM (
    SELECT
        parid,
        instruno,
        saledt,
        price,
        ROW_NUMBER()
            OVER (PARTITION BY parid ORDER BY saledt DESC)
            AS row_num
    FROM {{ source('iasworld', 'sales') }}
    WHERE saledt >= '2021-01-1'
        AND price > 1
) AS ranked_sales
WHERE row_num = 1
