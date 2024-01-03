-- QC view that selects rows from iasworld.sales joined against iasworld.pardat
-- to ensure that all parid values in iasworld.sales are in pardat.
-- We use this QC view instead of the `relationships` generic test since taxyr
-- is not a column on iasworld.sales and `relationships` can't handle the
-- necessary substring operation
SELECT
    sales.*,
    SUBSTR(sales.saledt, 1, 4) AS taxyr
FROM {{ source('iasworld', 'sales') }} AS sales
LEFT JOIN {{ source('iasworld', 'pardat') }} AS pardat
    ON sales.parid = pardat.parid
WHERE pardat.parid IS NULL
    AND SUBSTR(sales.saledt, 1, 4) >= '2011'
    AND sales.cur = 'Y'
    AND sales.deactivat IS NULL
