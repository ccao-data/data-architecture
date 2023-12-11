-- View that identifies sales with null values in key fields.
SELECT
    SUBSTR(sales.saledt, 1, 4) AS year,
    sales.price,
    sales.instrtyp AS deed_type,
    CASE WHEN LENGTH(sales.own1) < 2 THEN NULL ELSE sales.own1 END AS buyer,
    CASE WHEN LENGTH(sales.oldown) < 2 THEN NULL ELSE sales.oldown END AS seller
FROM {{ source('iasworld', 'sales') }} AS sales
WHERE
    sales.deactivat IS NULL
    AND sales.cur = 'Y'
    AND sales.instruno IS NOT NULL
    AND CAST(SUBSTR(sales.saledt, 1, 4) AS INTEGER) BETWEEN 2014 AND YEAR(
        CURRENT_DATE
    )
