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

unique_sales AS (
    SELECT
        *,
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
            SUBSTR(sales.saledt, 1, 4) AS year,
            tc.township_code,
            tc.nbhd,
            tc.class,
            DATE_PARSE(SUBSTR(sales.saledt, 1, 10), '%Y-%m-%d') AS sale_date,
            CAST(sales.price AS BIGINT) AS sale_price,
            sales.salekey AS sale_key,
            NULLIF(REPLACE(sales.instruno, 'D', ''), '') AS doc_no,
            NULLIF(sales.instrtyp, '') AS deed_type,
            -- "nopar" is number of parcels sold
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
            -- Sales are not entirely unique by pin/date so we group all
            -- sales by pin/date, then order by descending price
            -- and give the top observation a value of 1 for "max_price".
            -- We need to order by salekey as well in case of any ties within
            -- price, date, and pin.
            ROW_NUMBER() OVER (
                PARTITION BY
                    sales.parid,
                    sales.saledt,
                    sales.instrtyp NOT IN ('03', '04', '06')
                ORDER BY sales.price DESC, sales.salekey ASC
            ) AS max_price,
            -- We remove the letter 'D' that trails some document numbers in
            -- iasworld.sales since it prevents us from joining to mydec sales.
            -- This creates one instance where we have duplicate document
            -- numbers, so we sort by sale date (specifically to avoid conflicts
            -- with detecting the earliest duplicate sale when there are
            -- multiple within one document number, within a year) within the
            -- new document number to identify and remove the sale causing the
            -- duplicate document number.
            ROW_NUMBER() OVER (
                PARTITION BY
                    NULLIF(REPLACE(sales.instruno, 'D', ''), ''),
                    sales.instrtyp NOT IN ('03', '04', '06'),
                    sales.price > 10000
                ORDER BY sales.saledt ASC, sales.salekey ASC
            ) AS bad_doc_no,
            -- Some pins sell for the exact same price a few months after
            -- they're sold (we need to make sure to only include deed types we
            -- want). These sales are unnecessary for modeling and may be
            -- duplicates. We need to order by salekey as well in case of any
            -- ties within price, date, and pin.
            LAG(DATE_PARSE(SUBSTR(sales.saledt, 1, 10), '%Y-%m-%d')) OVER (
                PARTITION BY
                    sales.parid,
                    sales.price,
                    sales.instrtyp NOT IN ('03', '04', '06')
                ORDER BY sales.saledt ASC, sales.salekey ASC
            ) AS same_price_earlier_date,
            -- Historically, this view filtered out sales less than $10k and
            -- as well as quit claims, executor deeds, beneficial interests,
            -- and NULL deed types. Now we create "legacy" filter columns so
            -- that this filtering can reproduced while still allowing all sales
            -- into the view.
            sales.price <= 10000 AS sale_filter_less_than_10k,
            COALESCE(
                sales.instrtyp IN ('03', '04', '06') OR sales.instrtyp IS NULL,
                FALSE
            ) AS sale_filter_deed_type
        FROM {{ source('iasworld', 'sales') }} AS sales
        LEFT JOIN calculated
            ON NULLIF(REPLACE(sales.instruno, 'D', ''), '')
            = calculated.instruno
        LEFT JOIN
            town_class AS tc
            ON sales.parid = tc.parid
            AND SUBSTR(sales.saledt, 1, 4) = tc.taxyr
        WHERE sales.instruno IS NOT NULL
            AND sales.deactivat IS NULL
            AND sales.cur = 'Y'
            AND CAST(SUBSTR(sales.saledt, 1, 4) AS INT) BETWEEN 1997 AND YEAR(CURRENT_DATE)
            AND tc.township_code IS NOT NULL
            AND sales.price IS NOT NULL
    )
    WHERE max_price = 1
        AND (bad_doc_no = 1 OR is_multisale = TRUE)
),

mydec_sales AS (
    SELECT * FROM (
        SELECT
            REPLACE(document_number, 'D', '') AS doc_no,
            REPLACE(line_1_primary_pin, '-', '') AS pin,
            DATE_PARSE(line_4_instrument_date, '%Y-%m-%d') AS sale_date,
            SUBSTR(line_4_instrument_date, 1, 4) AS year,
            line_5_instrument_type AS mydec_deed_type,
            NULLIF(TRIM(seller_name), '') AS seller_name,
            NULLIF(TRIM(buyer_name), '') AS buyer_name,
            CAST(line_11_full_consideration AS BIGINT) AS sale_price,
            line_2_total_parcels AS num_parcels_sale,
            FALSE AS is_multisale,
            COALESCE(line_7_property_advertised = 1, FALSE)
                AS mydec_property_advertised,
            COALESCE(line_10a = 1, FALSE)
                AS mydec_is_installment_contract_fulfilled,
            COALESCE(line_10b = 1, FALSE)
                AS mydec_is_sale_between_related_individuals_or_corporate_affiliates,
            COALESCE(line_10c = 1, FALSE)
                AS mydec_is_transfer_of_less_than_100_percent_interest,
            COALESCE(line_10d = 1, FALSE)
                AS mydec_is_court_ordered_sale,
            COALESCE(line_10e = 1, FALSE)
                AS mydec_is_sale_in_lieu_of_foreclosure,
            COALESCE(line_10f = 1, FALSE)
                AS mydec_is_condemnation,
            COALESCE(line_10g = 1, FALSE)
                AS mydec_is_short_sale,
            COALESCE(line_10h = 1, FALSE)
                AS mydec_is_bank_reo_real_estate_owned,
            COALESCE(line_10i = 1, FALSE)
                AS mydec_is_auction_sale,
            COALESCE(line_10j = 1, FALSE)
                AS mydec_is_seller_buyer_a_relocation_company,
            COALESCE(line_10k = 1, FALSE)
                AS mydec_is_seller_buyer_a_financial_institution_or_government_agency,
            COALESCE(line_10l = 1, FALSE)
                AS mydec_is_buyer_a_real_estate_investment_trust,
            COALESCE(line_10m = 1, FALSE)
                AS mydec_is_buyer_a_pension_fund,
            COALESCE(line_10n = 1, FALSE)
                AS mydec_is_buyer_an_adjacent_property_owner,
            COALESCE(line_10o = 1, FALSE)
                AS mydec_is_buyer_exercising_an_option_to_purchase,
            COALESCE(line_10p = 1, FALSE)
                AS mydec_is_simultaneous_trade_of_property,
            COALESCE(line_10q = 1, FALSE)
                AS mydec_is_sale_leaseback,
            COALESCE(line_10s = 1, FALSE)
                AS mydec_is_homestead_exemption,
            line_10s_generalalternative
                AS mydec_homestead_exemption_general_alternative,
            line_10s_senior_citizens
                AS mydec_homestead_exemption_senior_citizens,
            line_10s_senior_citizens_assessment_freeze
                AS mydec_homestead_exemption_senior_citizens_assessment_freeze,
            -- Flag for booting outlier PTAX-203 sales from modeling and reporting
            (
                COALESCE(line_10b, 0) + COALESCE(line_10c, 0)
                + COALESCE(line_10d, 0) + COALESCE(line_10e, 0)
                + COALESCE(line_10f, 0) + COALESCE(line_10g, 0)
                + COALESCE(line_10h, 0) + COALESCE(line_10i, 0)
                + COALESCE(line_10k, 0)
            ) > 0 AS sale_filter_ptax_flag,
            COUNT() OVER (
                PARTITION BY line_1_primary_pin, line_4_instrument_date
            ) AS num_single_day_sales
        FROM {{ source('sale', 'mydec') }}
        WHERE line_2_total_parcels = 1 -- Remove multisales
    )
    WHERE num_single_day_sales = 1
        OR (YEAR(sale_date) > 2020)
),

max_version_flag AS (
    SELECT
        meta_sale_document_num,
        MAX(version) AS max_version
    FROM {{ source('sale', 'flag') }}
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
        {{ source('sale', 'flag') }} AS sf
    INNER JOIN max_version_flag AS mv
        ON sf.meta_sale_document_num = mv.meta_sale_document_num
        AND sf.version = mv.max_version
),
sales_full_outer AS (
    SELECT
        COALESCE(u.pin, m.pin) AS pin,
        COALESCE(u.year, m.year) AS year,
        COALESCE(u.township_code, tc.township_code) AS township_code,
        COALESCE(u.nbhd, tc.nbhd) AS nbhd,
        COALESCE(u.class, tc.class) AS class,
        COALESCE(u.sale_date, m.sale_date) AS sale_date,
        COALESCE(u.sale_price, m.sale_price) AS sale_price,
        u.sale_key,
        COALESCE(u.doc_no, m.doc_no) AS doc_no,
        COALESCE(u.deed_type, m.mydec_deed_type) AS deed_type,
        COALESCE(u.seller_name, m.seller_name) AS seller_name,
        COALESCE(u.is_multisale, m.is_multisale) AS is_multisale,
        COALESCE(u.num_parcels_sale, m.num_parcels_sale) AS num_parcels_sale,
        COALESCE(u.buyer_name, m.buyer_name) AS buyer_name,
        COALESCE(u.sale_type, NULL) AS sale_type,
        u.max_price,
        u.bad_doc_no,
        -- Compute 'same_price_earlier_date' for all sales
        LAG(COALESCE(u.sale_date, m.sale_date)) OVER (
            PARTITION BY COALESCE(u.pin, m.pin), COALESCE(u.sale_price, m.sale_price)
            ORDER BY COALESCE(u.sale_date, m.sale_date) ASC
        ) AS same_price_earlier_date,
        -- Compute 'sale_filter_less_than_10k'
        (COALESCE(u.sale_price, m.sale_price) <= 10000) AS sale_filter_less_than_10k,
        -- Compute 'sale_filter_deed_type'
        (COALESCE(u.deed_type, m.mydec_deed_type) IN ('03', '04', '06') OR COALESCE(u.deed_type, m.mydec_deed_type) IS NULL) AS sale_filter_deed_type,
        -- Compute 'sale_filter_same_sale_within_365'
        CASE
            WHEN LAG(COALESCE(u.sale_date, m.sale_date)) OVER (
                PARTITION BY COALESCE(u.pin, m.pin), COALESCE(u.sale_price, m.sale_price)
                ORDER BY COALESCE(u.sale_date, m.sale_date) ASC
            ) IS NOT NULL THEN
                EXTRACT(DAY FROM COALESCE(u.sale_date, m.sale_date) - LAG(COALESCE(u.sale_date, m.sale_date)) OVER (
                    PARTITION BY COALESCE(u.pin, m.pin), COALESCE(u.sale_price, m.sale_price)
                    ORDER BY COALESCE(u.sale_date, m.sale_date) ASC
                )) <= 365
            ELSE FALSE
        END AS sale_filter_same_sale_within_365,
        CASE WHEN u.doc_no IS NOT NULL THEN 'iasworld' ELSE 'mydec' END AS source,
        m.*
    FROM unique_sales u
    FULL OUTER JOIN mydec_sales m ON u.doc_no = m.doc_no
    LEFT JOIN town_class tc
        ON COALESCE(u.pin, m.pin) = tc.parid
        AND COALESCE(u.year, m.year) = tc.taxyr
)


SELECT
    sales_full_outer.pin,
    sales_full_outer.year,
    sales_full_outer.township_code,
    sales_full_outer.nbhd,
    sales_full_outer.class,
    sales_full_outer.sale_date,
    (sales_full_outer.source = 'mydec' OR YEAR(sales_full_outer.sale_date) >= 2021) AS is_mydec_date,
    sales_full_outer.sale_price,
    sales_full_outer.sale_key,
    sales_full_outer.doc_no,
    sales_full_outer.deed_type,
    sales_full_outer.seller_name,
    sales_full_outer.is_multisale,
    sales_full_outer.num_parcels_sale,
    sales_full_outer.buyer_name,
    sales_full_outer.sale_type,
    sales_full_outer.sale_filter_same_sale_within_365,
    sales_full_outer.sale_filter_less_than_10k,
    sales_full_outer.sale_filter_deed_type,
    COALESCE(sales_val.sv_is_outlier, FALSE) AS sale_filter_is_outlier,
    sales_full_outer.mydec_deed_type,
    sales_full_outer.sale_filter_ptax_flag,
    sales_full_outer.mydec_property_advertised,
    sales_full_outer.mydec_is_installment_contract_fulfilled,
    sales_full_outer.mydec_is_sale_between_related_individuals_or_corporate_affiliates,
    sales_full_outer.mydec_is_transfer_of_less_than_100_percent_interest,
    sales_full_outer.mydec_is_court_ordered_sale,
    sales_full_outer.mydec_is_sale_in_lieu_of_foreclosure,
    sales_full_outer.mydec_is_condemnation,
    sales_full_outer.mydec_is_short_sale,
    sales_full_outer.mydec_is_bank_reo_real_estate_owned,
    sales_full_outer.mydec_is_auction_sale,
    sales_full_outer.mydec_is_seller_buyer_a_relocation_company,
    sales_full_outer.mydec_is_seller_buyer_a_financial_institution_or_government_agency,
    sales_full_outer.mydec_is_buyer_a_real_estate_investment_trust,
    sales_full_outer.mydec_is_buyer_a_pension_fund,
    sales_full_outer.mydec_is_buyer_an_adjacent_property_owner,
    sales_full_outer.mydec_is_buyer_exercising_an_option_to_purchase,
    sales_full_outer.mydec_is_simultaneous_trade_of_property,
    sales_full_outer.mydec_is_sale_leaseback,
    sales_full_outer.mydec_is_homestead_exemption,
    sales_full_outer.mydec_homestead_exemption_general_alternative,
    sales_full_outer.mydec_homestead_exemption_senior_citizens,
    sales_full_outer.mydec_homestead_exemption_senior_citizens_assessment_freeze,
    sales_val.sv_is_outlier,
    sales_val.sv_is_ptax_outlier,
    sales_val.sv_is_heuristic_outlier,
    sales_val.sv_outlier_reason1,
    sales_val.sv_outlier_reason2,
    sales_val.sv_outlier_reason3,
    sales_val.sv_run_id,
    sales_val.sv_version,
    sales_full_outer.source
FROM sales_full_outer
LEFT JOIN sales_val
    ON sales_full_outer.doc_no = sales_val.meta_sale_document_num;