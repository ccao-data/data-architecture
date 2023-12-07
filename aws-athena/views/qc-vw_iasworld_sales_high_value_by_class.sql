-- View that identifies single-family house sales over $20m.
SELECT
    iasworld.parid AS pin,
    iasworld.price,
    res.class,
    res.year
FROM {{ source('iasworld', 'sales') }} AS iasworld
INNER JOIN {{ ref('default.vw_card_res_char') }} AS res
    ON iasworld.parid = res.pin AND res.year = SUBSTR(iasworld.saledt, 1, 4)
    AND res.class IN ('200', '202', '203', '204', '210')
WHERE iasworld.deactivat IS NULL
    AND iasworld.cur = 'Y'
    AND iasworld.instruno IS NOT NULL
    AND SUBSTR(iasworld.saledt, 1, 4) >= '2014'
    AND iasworld.price > 20000000
