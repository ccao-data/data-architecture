-- View that returns all MyDec sales from 2020 onward that do not have a
-- matching sale in iasWorld. Output is unique by document_number.

-- MyDec sales with no matching sale in iasWorld, found with a left anti join
-- on the digits-only document number
WITH mydec AS (
    SELECT
        mydec.document_number,
        mydec.line_4_instrument_date,
        mydec.full_address,
        mydec.line_5_instrument_type,
        mydec.line_11_full_consideration,
        mydec.line_3_additional_pins,
        SUBSTR(mydec.year_of_sale, 1, 4) AS year,
        REGEXP_REPLACE(mydec.line_1_primary_pin, '[^0-9]', '') AS pin,
        -- we expect that this should be unique by document number
        -- but we add this just to make sure
        ROW_NUMBER() OVER (
            PARTITION BY REGEXP_REPLACE(mydec.document_number, '[^0-9]', '')
            ORDER BY mydec.line_1_primary_pin
        ) AS doc_row_num
    FROM {{ source('sale', 'mydec') }} AS mydec
    LEFT JOIN {{ source('iasworld', 'sales') }} AS sales
        ON sales.deactivat IS NULL
        AND sales.cur = 'Y'
        AND REGEXP_REPLACE(sales.instruno, '[^0-9]', '')
        = REGEXP_REPLACE(mydec.document_number, '[^0-9]', '')
    WHERE SUBSTR(mydec.year_of_sale, 1, 4) >= '2020'
        AND sales.instruno IS NULL
)

SELECT
    mydec.document_number,
    mydec.line_4_instrument_date,
    -- Strip control characters that openpyxl refuses to write to Excel
    -- worksheets, since this free-text field can contain them
    REGEXP_REPLACE(
        mydec.full_address, '[\x{00}-\x{08}\x{0B}\x{0C}\x{0E}-\x{1F}]', ''
    ) AS mydec_address,
    mydec.pin AS primary_pin,
    pardat.class,
    mydec.line_5_instrument_type,
    mydec.line_11_full_consideration,
    mydec.line_3_additional_pins AS multi_pin
FROM mydec
LEFT JOIN {{ source('iasworld', 'pardat') }} AS pardat
    ON mydec.pin = pardat.parid
    AND mydec.year = pardat.taxyr
    AND pardat.cur = 'Y'
    AND pardat.deactivat IS NULL
WHERE mydec.doc_row_num = 1
