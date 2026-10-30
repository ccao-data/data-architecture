-- View that returns all MyDec sales that do not have a matching sale in
-- iasWorld.
SELECT mydec.*
FROM {{ source('sale', 'mydec') }} AS mydec
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ source('iasworld', 'sales') }} AS sales
    WHERE sales.deactivat IS NULL
        AND sales.cur = 'Y'
        -- MyDec document numbers are stored as digits only, so strip
        -- non-digit characters from iasWorld instrument numbers to match
        AND REGEXP_REPLACE(sales.instruno, '[^0-9]', '')
        = mydec.document_number
)
