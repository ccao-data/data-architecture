WITH 
-- "nopar" isn't entirely accurate for sales associated with only one parcel,
-- so we create our own counter
calculated AS (
    SELECT
        instruno,
        COUNT(*) AS nopar_calculated
    FROM (
        SELECT DISTINCT
            parid,
            NULLIF(REPLACE(instruno, 'D', ''), '') AS instruno
        FROM {{ source('iasworld', 'sales') }}
        WHERE deactivat IS NULL
            AND cur = 'Y'
    )
    GROUP BY instruno
),

ias_sales AS (
    SELECT
        parid as pin,
        NULLIF(REPLACE(sales.instruno, 'D', ''), '') as doc_no,
        DATE_PARSE(SUBSTR(sales.saledt, 1, 10), '%Y-%m-%d') AS sale_date,
        CAST(sales.price AS BIGINT) AS sale_price,
        COALESCE(
            sales.nopar > 1 OR calculated.nopar_calculated > 1,
            FALSE
        ) AS is_multisale,
        CASE
            WHEN sales.nopar > 1 THEN sales.nopar ELSE
                calculated.nopar_calculated
        END AS num_parcels_sale,
        CASE WHEN TRIM(oldown) IN ('', 'MISSING SELLER NAME')
                THEN NULL   
            ELSE oldown
        END AS seller_name,
        CASE WHEN TRIM(own1) IN ('', 'MISSING BUYER NAME')
                THEN NULL
            ELSE own1
        END AS buyer_name
    FROM iasworld.sales
    WHERE
        deactivat IS null
        and cur = 'Y'
    LEFT JOIN calculated.doc_no = calculated.instruno
),
mydec_sales AS (
    SELECT
        REPLACE(line_1_primary_pin, '-', '') AS pin,
        REPLACE(document_number, 'D', '') AS doc_no,
        DATE_PARSE(line_4_instrument_date, '%Y-%m-%d') AS sale_date,
        line_11_full_consideration as sale_price,
        NULLIF(TRIM(seller_name), '') AS seller_name,
        NULLIF(TRIM(buyer_name), '') AS buyer_name,
        COALESCE(
            line_2_total_parcels > 1
            FALSE
        ) AS is_multisale,
        line_2_total_parcels as num_parcels_sale
    FROM sale.mydec
),

WITH combined_sales AS (
    -- Select all rows from ias_sales
    SELECT 
        ias.pin,
        ias.doc_no,
        COALESCE(mydec.sale_date, ias.sale_date) AS sale_date,
        CASE WHEN mydec.sale_date IS NOT NULL THEN TRUE ELSE FALSE END AS is_mydec_date,
        ias.sale_price,
        ias.seller_name,
        ias.buyer_name,
        ias.is_multisale
        ias.num_parcels_sale
        'iasworld' AS source
    FROM ias_sales ias
    LEFT JOIN mydec_sales mydec ON ias.doc_no = mydec.doc_no

    UNION ALL

    -- Select rows from mydec_sales that don't exist in ias_sales
    SELECT 
        mydec.pin,
        mydec.doc_no,
        mydec.sale_date,
        TRUE AS is_mydec_date,
        mydec.sale_price,
        mydec.seller_name,
        mydec.buyer_name,
        mydec.num_parcels_sale
        'mydec' AS source
    FROM mydec_sales mydec
    LEFT JOIN ias_sales ias ON mydec.doc_no = ias.doc_no
    WHERE ias.doc_no IS NULL
)

SELECT * FROM combined_sales