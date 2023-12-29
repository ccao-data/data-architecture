-- View that identifies class 2 sales over $20m.
SELECT
    sales.parid,
    sales.price,
    par.class,
    COUNT(*) OVER (PARTITION BY sales.instruno) AS parcel_count,
    par.taxyr
FROM {{ source('iasworld', 'sales') }} AS sales
INNER JOIN {{ source('iasworld', 'pardat') }} AS par
    ON sales.parid = par.parid AND par.taxyr = SUBSTR(sales.saledt, 1, 4)
    AND SUBSTR(par.class, 1, 1) = '2' AND par.class != '299'
WHERE sales.deactivat IS NULL
    AND sales.cur = 'Y'
    AND sales.instruno IS NOT NULL
    AND SUBSTR(sales.saledt, 1, 4) >= '2014'
