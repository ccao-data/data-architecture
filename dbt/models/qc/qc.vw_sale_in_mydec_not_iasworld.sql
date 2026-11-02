-- View that returns all MyDec sales from 2020 onward that do not have a
-- matching sale in iasWorld.
SELECT
    mydec.document_number,
    mydec.line_4_instrument_date,
    mydec.full_address,
    mydec.line_1_primary_pin,
    mydec.line_5_instrument_type,
    mydec.line_11_full_consideration
FROM {{ source('sale', 'mydec') }} AS mydec
WHERE SUBSTR(mydec.year_of_sale, 1, 4) >= '2020'
    AND NOT EXISTS (
        SELECT 1
        FROM {{ source('iasworld', 'sales') }} AS sales
        WHERE sales.deactivat IS NULL
            AND sales.cur = 'Y'
            -- MyDec document numbers are stored as digits only, so strip
            -- non-digit characters from iasWorld instrument numbers to match
            AND REGEXP_REPLACE(sales.instruno, '[^0-9]', '')
            = mydec.document_number
    )
