WITH sales_cte AS (
    SELECT
        parid,
        price,
        CAST(SUBSTR(saledt, 1, 4) AS INTEGER) AS year_of_sale
    FROM iasworld.sales
    WHERE
        deactivat IS NULL
        AND cur = 'Y'
        AND instruno IS NOT NULL
        AND CAST(SUBSTR(saledt, 1, 4) AS INTEGER) >= 2014
        AND price IS NOT NULL
),

res_char AS (
    SELECT
        pin,
        class,
        CAST(year AS INTEGER) AS year
    FROM default.vw_card_res_char
    WHERE
        CAST(year AS INTEGER) >= 2014
        AND class IN ('200', '202', '203', '204', '210')
)

SELECT
    iasworld.parid,
    iasworld.price,
    res.pin,
    res.class,
    res.year,
    iasworld.year_of_sale
FROM sales_cte AS iasworld
INNER JOIN
    res_char AS res
    ON iasworld.parid = res.pin AND res.year = iasworld.year_of_sale;
