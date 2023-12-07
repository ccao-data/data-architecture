-- View that contains all matched iasWorld and MyDec sales with unequal sale
-- prices.
SELECT
    REGEXP_REPLACE(iasworld.instruno, '[^0-9]', '') AS document_number,
    SUBSTR(iasworld.saledt, 1, 4) AS year_iasworld,
    iasworld.price AS price_iasworld,
    SUBSTR(mydec.year_of_sale, 1, 4) AS year_mydec,
    mydec.line_11_full_consideration AS price_mydec
FROM {{ source('iasworld', 'sales') }} AS iasworld
FULL OUTER JOIN {{ source('sale', 'mydec') }} AS mydec
    ON SUBSTR(iasworld.saledt, 1, 4) = SUBSTR(mydec.year_of_sale, 1, 4)
    AND REGEXP_REPLACE(iasworld.instruno, '[^0-9]', '') = mydec.document_number
    AND SUBSTR(mydec.year_of_sale, 1, 4) >= '2014'
WHERE iasworld.price != mydec.line_11_full_consideration
    AND iasworld.deactivat IS NULL
    AND iasworld.cur = 'Y'
    AND SUBSTR(iasworld.saledt, 1, 4) >= '2014'
