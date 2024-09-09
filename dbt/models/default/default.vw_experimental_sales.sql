-- View containing unique, filtered sales
-- Class and township of associated PIN
WITH town_class AS (
    SELECT
        par.parid,
        REGEXP_REPLACE(par.class, '[^[:alnum:]]', '') AS class,
        par.taxyr,
        leg.user1 AS township_code,
        CONCAT(
            leg.user1, SUBSTR(REGEXP_REPLACE(par.nbhd, '([^0-9])', ''), 3, 3)
        ) AS nbhd
    FROM {{ source('iasworld', 'pardat') }} AS par
    LEFT JOIN {{ source('iasworld', 'legdat') }} AS leg
        ON par.parid = leg.parid
        AND par.taxyr = leg.taxyr
        AND leg.cur = 'Y'
        AND leg.deactivat IS NULL
    WHERE par.cur = 'Y'
        AND par.deactivat IS NULL
),

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

all_mydec_sales AS (
    SELECT
        REPLACE(document_number, 'D', '') AS doc_no,
        REPLACE(line_1_primary_pin, '-', '') AS pin,
        DATE_PARSE(line_4_instrument_date, '%Y-%m-%d') AS mydec_date,
        NULLIF(TRIM(seller_name), '') AS seller_name,
        NULLIF(TRIM(buyer_name), '') AS buyer_name,
        CAST(line_11_full_consideration AS BIGINT) AS sale_price,
        year_of_sale,
        line_2_total_parcels
    FROM {{ source('sale', 'mydec') }}
),

unique_sales AS (
    SELECT
        COALESCE(iasw.parid, mydec.pin) AS pin,
        COALESCE(iasw.year, mydec.year_of_sale) AS year,
        tc.township_code,
        tc.nbhd,
        tc.class,
        COALESCE(iasw.iasw_sale_date, mydec.mydec_date) AS sale_date,
        COALESCE(iasw.sale_price, mydec.sale_price) AS sale_price,
        iasw.sale_key,
        COALESCE(iasw.doc_no, mydec.doc_no) AS doc_no,
        iasw.deed_type,
        COALESCE(iasw.seller_name, mydec.seller_name) AS seller_name,
        COALESCE(iasw.buyer_name, mydec.buyer_name) AS buyer_name,
        COALESCE(iasw.is_multisale, mydec.line_2_total_parcels > 1)
            AS is_multisale,
        COALESCE(iasw.num_parcels_sale, mydec.line_2_total_parcels)
            AS num_parcels_sale,
        iasw.sale_type,
        iasw.sale_filter_same_sale_within_365,
        iasw.sale_filter_less_than_10k,
        iasw.sale_filter_deed_type,
        CASE
            WHEN iasw.parid IS NULL THEN 'MyDec'
            WHEN mydec.pin IS NULL THEN 'iasWorld'
            ELSE 'Both'
        END AS data_source,
        CASE
            WHEN iasw.doc_no IS NOT NULL THEN 'iasWorld'
            ELSE 'MyDec'
        END AS source_sale
    FROM (
        SELECT
            sales.parid,
            SUBSTR(sales.saledt, 1, 4) AS year,
            DATE_PARSE(SUBSTR(sales.saledt, 1, 10), '%Y-%m-%d') AS iasw_sale_date,
            CAST(sales.price AS BIGINT) AS sale_price,
            sales.salekey AS sale_key,
            NULLIF(REPLACE(sales.instruno, 'D', ''), '') AS doc_no,
            NULLIF(sales.instrtyp, '') AS deed_type,
            COALESCE(
                sales.nopar > 1 OR calculated.nopar_calculated > 1,
                FALSE
            ) AS is_multisale,
            CASE
                WHEN sales.nopar > 1 THEN sales.nopar ELSE
                    calculated.nopar_calculated
            END AS num_parcels_sale,
            CASE WHEN TRIM(sales.oldown) IN ('', 'MISSING SELLER NAME')
                    THEN NULL
                ELSE sales.oldown
            END AS seller_name,
            CASE WHEN TRIM(sales.own1) IN ('', 'MISSING BUYER NAME')
                    THEN NULL
                ELSE sales.own1
            END AS buyer_name,
            CASE
                WHEN sales.saletype = '0' THEN 'LAND'
                WHEN sales.saletype = '1' THEN 'LAND AND BUILDING'
            END AS sale_type,
            COALESCE(
                EXTRACT(DAY FROM sale_date - LAG(sale_date) OVER (
                    PARTITION BY sales.parid, sales.price
                    ORDER BY sale_date
                )) <= 365,
                FALSE
            ) AS sale_filter_same_sale_within_365,
            sales.price <= 10000 AS sale_filter_less_than_10k,
            COALESCE(
                sales.instrtyp IN ('03', '04', '06') OR sales.instrtyp IS NULL,
                FALSE
            ) AS sale_filter_deed_type
        FROM {{ source('iasworld', 'sales') }} AS sales
        LEFT JOIN calculated
            ON NULLIF(REPLACE(sales.instruno, 'D', ''), '')
            = calculated.instruno
        WHERE sales.instruno IS NOT NULL
            AND sales.deactivat IS NULL
            AND sales.cur = 'Y'
            AND CAST(SUBSTR(sales.saledt, 1, 4) AS INT) BETWEEN 1997 AND YEAR(
                CURRENT_DATE
            )
            AND sales.price IS NOT NULL
    ) AS iasw
    FULL OUTER JOIN all_mydec_sales AS mydec
        ON iasw.doc_no = mydec.doc_no
    LEFT JOIN town_class AS tc
        ON COALESCE(iasw.parid, mydec.pin) = tc.parid
        AND COALESCE(iasw.year, mydec.year_of_sale) = tc.taxyr
    WHERE tc.township_code IS NOT NULL
)

SELECT
    pin,
    year,
    township_code,
    nbhd,
    class,
    sale_date,
    sale_price,
    sale_key,
    doc_no,
    deed_type,
    seller_name,
    buyer_name,
    is_multisale,
    num_parcels_sale,
    sale_type,
    sale_filter_same_sale_within_365,
    sale_filter_less_than_10k,
    sale_filter_deed_type,
    data_source
FROM unique_sales;
