-- Class and township of associated PIN
WITH town_class AS (
    SELECT
        par.parid,
        REGEXP_REPLACE(par.class, '[^[:alnum:]]', '') AS class,
        par.taxyr,
        leg.user1 AS township_code,
        td.township_name,
        CONCAT(
            leg.user1, SUBSTR(REGEXP_REPLACE(par.nbhd, '([^0-9])', ''), 3, 3)
        ) AS nbhd
    FROM iasworld.pardat AS par
    LEFT JOIN iasworld.legdat AS leg
        ON par.parid = leg.parid
        AND par.taxyr = leg.taxyr
        AND leg.cur = 'Y'
        AND leg.deactivat IS NULL
    LEFT JOIN (
        SELECT DISTINCT township_name, township_code
        FROM default.vw_pin_universe
    ) AS td
        ON leg.user1 = td.township_code
    WHERE par.cur = 'Y'
        AND par.deactivat IS NULL
),

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
    SELECT *,
        -- Historically, this view excluded sales for a given pin if it had sold
        -- within the last 12 months for the same price. This filter allows us
        -- to filter out those sales.
        COALESCE(
            EXTRACT(DAY FROM sale_date - same_price_earlier_date) <= 365,
            FALSE
        ) AS sale_filter_same_sale_within_365
    FROM (
        SELECT
            sales.parid AS pin,
            NULLIF(REPLACE(sales.instruno, 'D', ''), '') AS doc_no,
            tc.class,
            tc.township_code,
            SUBSTR(sales.saledt, 1, 4) AS year,
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
            END AS buyer_name,
            ROW_NUMBER() OVER (
                PARTITION BY
                    sales.parid,
                    sales.saledt,
                    sales.instrtyp NOT IN ('03', '04', '06')
                ORDER BY sales.price DESC, sales.salekey ASC
            ) AS max_price,
            ROW_NUMBER() OVER (
                PARTITION BY
                    NULLIF(REPLACE(sales.instruno, 'D', ''), ''),
                    sales.instrtyp NOT IN ('03', '04', '06'),
                    sales.price > 10000
                ORDER BY sales.saledt ASC, sales.salekey ASC
            ) AS bad_doc_no,
            LAG(DATE_PARSE(SUBSTR(sales.saledt, 1, 10), '%Y-%m-%d')) OVER (
                PARTITION BY
                    sales.parid,
                    sales.price,
                    sales.instrtyp NOT IN ('03', '04', '06')
                ORDER BY sales.saledt ASC, sales.salekey ASC
            ) AS same_price_earlier_date,
            sales.price <= 10000 AS sale_filter_less_than_10k
        FROM iasworld.sales AS sales
        LEFT JOIN
            calculated
            ON calculated.instruno = NULLIF(REPLACE(sales.instruno, 'D', ''), '')
        LEFT JOIN
            town_class AS tc
            ON sales.parid = tc.parid
            AND SUBSTR(sales.saledt, 1, 4) = tc.taxyr
        WHERE
            sales.deactivat IS NULL
            AND sales.cur = 'Y'
            AND CAST(SUBSTR(sales.saledt, 1, 4) AS INT) BETWEEN 1997 AND YEAR(
                CURRENT_DATE
            )
            AND tc.township_code IS NOT NULL
            AND sales.price IS NOT NULL
    ) AS subquery
    -- Only use max price by pin/sale date
    WHERE max_price = 1
        AND (bad_doc_no = 1 OR is_multisale = TRUE)
),

mydec_sales AS (
    SELECT *
    FROM (
        SELECT
            REPLACE(line_1_primary_pin, '-', '') AS pin,
            REPLACE(document_number, 'D', '') AS doc_no,
            tc.class,
            tc.township_code,
            SUBSTR(line_4_instrument_date, 1, 4) AS year,
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
            year_of_sale,
            line_11_full_consideration <= 10000 AS sale_filter_less_than_10k
        FROM sale.mydec
        LEFT JOIN
            town_class AS tc
            ON REPLACE(line_1_primary_pin, '-', '') = tc.parid
            AND SUBSTR(mydec.line_4_instrument_date, 1, 4) = tc.taxyr
        WHERE line_2_total_parcels = 1 -- Remove multisales
            AND tc.township_code IS NOT NULL
            AND line_11_full_consideration IS NOT NULL
    ) AS derived_table
    WHERE num_single_day_sales = 1
        OR (YEAR(sale_date) > 2020)
),

max_version_flag AS (
    SELECT
        meta_sale_document_num,
        MAX(version) AS max_version
    FROM sale.flag
    GROUP BY meta_sale_document_num
),

sales_val AS (
    SELECT
        sf.meta_sale_document_num,
        sf.sv_is_outlier,
        sf.sv_is_ptax_outlier,
        sf.sv_is_heuristic_outlier,
        sf.sv_outlier_reason1,
        sf.sv_outlier_reason2,
        sf.sv_outlier_reason3,
        sf.run_id AS sv_run_id,
        sf.version AS sv_version
    FROM
        sale.flag AS sf
    INNER JOIN max_version_flag AS mv
        ON sf.meta_sale_document_num = mv.meta_sale_document_num
        AND sf.version = mv.max_version
),

combined_sales AS (
    -- Select all rows from ias_sales
    SELECT
        ias.pin,
        ias.doc_no,
        ias.township_code,
        ias.class,
        ias.year,
        COALESCE(mydec.sale_date, ias.sale_date) AS sale_date,
        COALESCE(mydec.sale_date IS NOT NULL, FALSE)
            AS is_mydec_date,
        ias.sale_price,
        ias.seller_name,
        ias.buyer_name,
        ias.is_multisale,
        ias.num_parcels_sale,
        ias.sale_filter_less_than_10k,
        'iasworld' AS source,
        sales_val.sv_is_outlier,
        sales_val.sv_is_ptax_outlier,
        sales_val.sv_is_heuristic_outlier,
        sales_val.sv_outlier_reason1,
        sales_val.sv_outlier_reason2,
        sales_val.sv_outlier_reason3,
        sales_val.sv_run_id,
        sales_val.sv_version
    FROM ias_sales AS ias
    LEFT JOIN mydec_sales AS mydec ON ias.doc_no = mydec.doc_no
    LEFT JOIN sales_val
    ON ias.doc_no = sales_val.meta_sale_document_num

    UNION ALL

    -- Select rows from mydec_sales that don't exist in ias_sales
    SELECT
        mydec.pin,
        mydec.doc_no,
        mydec.township_code,
        mydec.class,
        mydec.year,
        mydec.sale_date,
        TRUE AS is_mydec_date,
        mydec.sale_price,
        mydec.seller_name,
        mydec.buyer_name,
        mydec.is_multisale,
        mydec.num_parcels_sale,
        mydec.sale_filter_less_than_10k,
        'mydec' AS source,
        sales_val.sv_is_outlier,
        sales_val.sv_is_ptax_outlier,
        sales_val.sv_is_heuristic_outlier,
        sales_val.sv_outlier_reason1,
        sales_val.sv_outlier_reason2,
        sales_val.sv_outlier_reason3,
        sales_val.sv_run_id,
        sales_val.sv_version
    FROM mydec_sales AS mydec
    LEFT JOIN sales_val
    ON mydec.doc_no = sales_val.meta_sale_document_num
    LEFT JOIN ias_sales AS ias ON mydec.doc_no = ias.doc_no
    WHERE ias.doc_no IS NULL
)

SELECT * FROM combined_sales