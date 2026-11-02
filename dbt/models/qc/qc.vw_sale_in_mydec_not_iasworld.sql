-- View that returns all MyDec sales from 2020 onward that do not have a
-- matching sale in iasWorld. Output is unique by document_number.
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
        REGEXP_REPLACE(mydec.line_1_primary_pin, '[^0-9]', '') AS pin,
        -- Normalize addresses so that MyDec unit numbers (e.g. "# 6S")
        -- match the iasWorld format (e.g. "6S")
        TRIM(REGEXP_REPLACE(
            REPLACE(UPPER(mydec.line_1_street), '#', ''), '\s+', ' '
        )) AS address_normalized,
        UPPER(TRIM(mydec.line_1_city)) AS city,
        SUBSTR(mydec.line_1_zip_code, 1, 5) AS zip
    FROM {{ source('sale', 'mydec') }} AS mydec
    WHERE SUBSTR(mydec.year_of_sale, 1, 4) >= '2020'
),

mydec_all AS (
    SELECT
        mydec_clean.*,
        -- Multi-sales have one row per PIN in MyDec. Keep one row per
        -- document (the lowest PIN) so the view is unique by document number
        ROW_NUMBER() OVER (
            PARTITION BY mydec_clean.document_number_clean
            ORDER BY mydec_clean.line_1_primary_pin
        ) AS doc_row_num
    FROM mydec_clean
    WHERE NOT EXISTS (
        SELECT 1
        FROM {{ source('iasworld', 'sales') }} AS sales
        WHERE sales.deactivat IS NULL
            AND sales.cur = 'Y'
            -- Strip non-digit characters (e.g. the "D" suffix) from iasWorld
            -- instrument numbers too, so both sides are compared as digits
            AND REGEXP_REPLACE(sales.instruno, '[^0-9]', '')
            = mydec_clean.document_number_clean
    )
),

mydec AS (
    SELECT *
    FROM mydec_all
    WHERE doc_row_num = 1
),

pin_address AS (
    SELECT
        pin,
        year,
        TRIM(REGEXP_REPLACE(UPPER(prop_address_full), '\s+', ' '))
            AS address_normalized,
        UPPER(TRIM(prop_address_city_name)) AS city,
        prop_address_zipcode_1 AS zip
    FROM {{ ref('default.vw_pin_address') }}
    WHERE year >= '2020'
),

-- PINs whose iasWorld property address matches the MyDec address in the
-- year of sale. More than one PIN can match, e.g. condo units in a building
-- when the MyDec address has no unit number
address_match AS (
    SELECT
        mydec.document_number_clean,
        ARRAY_JOIN(ARRAY_AGG(DISTINCT pin_address.pin), ', ')
            AS address_matched_pins
    FROM mydec
    INNER JOIN pin_address
        ON mydec.address_normalized = pin_address.address_normalized
        AND mydec.city = pin_address.city
        AND mydec.zip = pin_address.zip
        AND mydec.year = pin_address.year
    GROUP BY mydec.document_number_clean
)

SELECT
    mydec.document_number,
    mydec.line_4_instrument_date,
    -- Strip control characters that openpyxl refuses to write to Excel
    -- worksheets, since this free-text field can contain them
    REGEXP_REPLACE(
        mydec.full_address, '[\x{00}-\x{08}\x{0B}\x{0C}\x{0E}-\x{1F}]', ''
    ) AS full_address,
    mydec.line_1_primary_pin,
    pardat.class,
    address_match.address_matched_pins,
    mydec.line_5_instrument_type,
    mydec.line_11_full_consideration
FROM mydec
LEFT JOIN {{ source('iasworld', 'pardat') }} AS pardat
    ON mydec.pin = pardat.parid
    AND mydec.year = pardat.taxyr
    AND pardat.cur = 'Y'
    AND pardat.deactivat IS NULL
LEFT JOIN address_match
    ON mydec.document_number_clean = address_match.document_number_clean
