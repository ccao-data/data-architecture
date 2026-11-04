-- View that returns all MyDec sales from 2020 onward that do not have a
-- matching sale in iasWorld. Output is unique by document_number.

-- MyDec sales from 2020 onward, with the document number, PIN, and address.
WITH mydec_clean AS (
    SELECT
        mydec.document_number,
        -- Digits-only document number, used for all matching so that
        -- formatting differences don't prevent a match. Not output
        REGEXP_REPLACE(mydec.document_number, '[^0-9]', '')
            AS document_number_clean,
        mydec.line_1_primary_pin,
        mydec.line_4_instrument_date,
        mydec.full_address,
        mydec.line_5_instrument_type,
        mydec.line_11_full_consideration,
        SUBSTR(mydec.year_of_sale, 1, 4) AS year,
        REGEXP_REPLACE(mydec.line_1_primary_pin, '[^0-9]', '') AS pin
    FROM {{ source('sale', 'mydec') }} AS mydec
    WHERE SUBSTR(mydec.year_of_sale, 1, 4) >= '2020'
),

-- MyDec sales with no matching sale in iasWorld, found with a left anti join
-- on the digits-only document number
mydec AS (
    SELECT
        mydec_clean.*,
        -- Multi-sales have one row per PIN in MyDec. Number the rows in each
        -- document so that we can keep one row per document number
        ROW_NUMBER() OVER (
            PARTITION BY mydec_clean.document_number_clean
            ORDER BY mydec_clean.line_1_primary_pin
        ) AS doc_row_num
    FROM mydec_clean
    LEFT JOIN {{ source('iasworld', 'sales') }} AS sales
        ON sales.deactivat IS NULL
        AND sales.cur = 'Y'
        -- Strip non-digit characters from iasWorld
        AND REGEXP_REPLACE(sales.instruno, '[^0-9]', '')
        = mydec_clean.document_number_clean
    WHERE sales.instruno IS NULL
)

SELECT
    mydec.document_number,
    mydec.line_4_instrument_date,
    -- Strip control characters that openpyxl refuses to write to Excel
    -- worksheets, since this free-text field can contain them
    REGEXP_REPLACE(
        mydec.full_address, '[\x{00}-\x{08}\x{0B}\x{0C}\x{0E}-\x{1F}]', ''
    ) AS mydec_address,
    mydec.pin AS line_1_primary_pin,
    pardat.class,
    mydec.line_5_instrument_type,
    mydec.line_11_full_consideration
FROM mydec
LEFT JOIN {{ source('iasworld', 'pardat') }} AS pardat
    ON mydec.pin = pardat.parid
    AND mydec.year = pardat.taxyr
    AND pardat.cur = 'Y'
    AND pardat.deactivat IS NULL
WHERE mydec.doc_row_num = 1
