-- View that identifies sales with null values in key fields.
SELECT
    SUBSTR(sales.saledt, 1, 4) AS year,
    CAST(
        COUNT(
            CASE WHEN sales.price IS NULL THEN 1 END
        ) AS DOUBLE
    )
    / CAST(COUNT(*) AS DOUBLE) AS price,
    CAST(
        COUNT(
            CASE WHEN sales.instrtyp IS NULL THEN 1 END
        ) AS DOUBLE
    )
    / CAST(COUNT(*) AS DOUBLE) AS deed_type,
    CAST(
        COUNT(
            CASE
                WHEN
                    LENGTH(sales.own1) < 2 OR sales.own1 IS NULL
                    THEN 1
            END
        ) AS DOUBLE
    )
    / CAST(COUNT(*) AS DOUBLE) AS buyer,
    CAST(
        COUNT(
            CASE
                WHEN
                    LENGTH(sales.oldown) < 2 OR sales.oldown IS NULL
                    THEN 1
            END
        ) AS DOUBLE
    )
    / CAST(COUNT(*) AS DOUBLE) AS seller
FROM {{ source('iasworld', 'sales') }} AS sales
WHERE
    sales.deactivat IS NULL
    AND sales.cur = 'Y'
    AND sales.instruno IS NOT NULL
    AND SUBSTR(sales.saledt, 1, 4) >= '2014'
GROUP BY SUBSTR(sales.saledt, 1, 4)
