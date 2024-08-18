SELECT
    parid,
    instruno,
    price,
    saledt,
    DATE_FORMAT(
        DATE_PARSE(saledt, '%Y-%m-%d %H:%i:%S.%f'), '%c/%e/%Y'
    ) AS saledt_fmt
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
    WHERE price > 1
) AS ranked_sales
WHERE row_num = 1
