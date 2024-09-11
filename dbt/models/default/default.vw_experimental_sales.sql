WITH
calculated AS (
    SELECT
        instruno,
        COUNT(*) AS nopar_calculated
    FROM (
        SELECT DISTINCT
            parid,
            NULLIF(REPLACE(instruno, 'D', ''), '') AS instruno
        FROM iasworld.sales
        WHERE deactivat IS NULL
            AND cur = 'Y'
    ) AS distinct_sales
    GROUP BY instruno
),

ias_sales AS (
    SELECT
        sales.parid AS pin,
        NULLIF(REPLACE(sales.instruno, 'D', ''), '') AS doc_no,
        DATE_PARSE(SUBSTR(sales.saledt, 1, 10), '%Y-%m-%d') AS sale_date,
        CAST(sales.price AS BIGINT) AS sale_price,
        COALESCE(
            (sales.nopar > 1 OR calculated.nopar_calculated > 1),
            FALSE
        ) AS is_multisale,
        CASE
            WHEN sales.nopar > 1 THEN sales.nopar
            ELSE calculated.nopar_calculated
        END AS num_parcels_sale,
        CASE
            WHEN TRIM(sales.oldown) IN ('', 'MISSING SELLER NAME')
                THEN NULL
            ELSE sales.oldown
        END AS seller_name,
        CASE
            WHEN TRIM(sales.own1) IN ('', 'MISSING BUYER NAME')
                THEN NULL
            ELSE sales.own1
        END AS buyer_name
    FROM iasworld.sales AS sales
    LEFT JOIN
        calculated
        ON calculated.instruno = NULLIF(REPLACE(sales.instruno, 'D', ''), '')
    WHERE
        sales.deactivat IS NULL
        AND sales.cur = 'Y'
),

mydec_sales AS (
    SELECT *
    FROM (
        SELECT
            REPLACE(document_number, 'D', '') AS doc_no,
            REPLACE(line_1_primary_pin, '-', '') AS pin,
            DATE_PARSE(line_4_instrument_date, '%Y-%m-%d') AS sale_date,
            line_11_full_consideration AS sale_price,
            NULLIF(TRIM(seller_name), '') AS seller_name,
            NULLIF(TRIM(buyer_name), '') AS buyer_name,
            COALESCE(
                line_2_total_parcels > 1,
                FALSE
            ) AS is_multisale,
            line_2_total_parcels AS num_parcels_sale,
            COUNT(*) OVER (
                PARTITION BY line_1_primary_pin, line_4_instrument_date
            ) AS num_single_day_sales,
            year_of_sale
        FROM sale.mydec
        WHERE line_2_total_parcels = 1 -- Remove multisales
    ) AS derived_table
    WHERE num_single_day_sales = 1
        OR (YEAR(sale_date) > 2020)
),

combined_sales AS (
    -- Select all rows from ias_sales
    SELECT
        ias.pin,
        ias.doc_no,
        COALESCE(mydec.sale_date, ias.sale_date) AS sale_date,
        COALESCE(mydec.sale_date IS NOT NULL, FALSE)
            AS is_mydec_date,
        ias.sale_price,
        ias.seller_name,
        ias.buyer_name,
        ias.is_multisale,
        ias.num_parcels_sale,
        'iasworld' AS source
    FROM ias_sales AS ias
    LEFT JOIN mydec_sales AS mydec ON ias.doc_no = mydec.doc_no

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
        mydec.is_multisale,
        mydec.num_parcels_sale,
        'mydec' AS source
    FROM mydec_sales AS mydec
    LEFT JOIN ias_sales AS ias ON mydec.doc_no = ias.doc_no
    WHERE ias.doc_no IS NULL
)

SELECT * FROM combined_sales
