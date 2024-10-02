-- View containing unique, filtered sales

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
            DATE_DIFF(
                'day',
                same_price_earlier_date,
                sale_date
            ) <= 365,
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
            -- with detecting the easliest duplicate sale when there are
            -- multiple within one document number, within a year) within the
            -- new doument number to identify and remove the sale causing the
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
            -- want). These sales are unecessary for modeling and may be
            -- duplicates. We need to order by salekey as well in case of any
            -- ties within price, date, and pin.
            LAG(DATE_PARSE(SUBSTR(sales.saledt, 1, 10), '%Y-%m-%d')) OVER (
                PARTITION BY
                    sales.parid,
                    sales.price,
                    sales.instrtyp NOT IN ('03', '04', '06')
                ORDER BY sales.saledt ASC, sales.salekey ASC
            ) AS same_price_earlier_date,
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
            AND CAST(SUBSTR(sales.saledt, 1, 4) AS INT) BETWEEN 1997 AND YEAR(
                CURRENT_DATE
            )
            AND tc.township_code IS NOT NULL
            AND sales.price IS NOT NULL
    )
    WHERE max_price = 1
        AND (bad_doc_no = 1 OR is_multisale = TRUE)
),

mydec_sales AS (
    SELECT *,
        COALESCE(
            DATE_DIFF(
                'day',
                LAG(sale_date) OVER (
                    PARTITION BY pin
                    ORDER BY sale_date ASC
                ),
                sale_date
            ) <= 365,
            FALSE
        ) AS sale_filter_same_sale_within_365
    FROM (
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
                COALESCE(line_2_total_parcels > 1, FALSE) AS is_multisale,
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
            WHERE line_2_total_parcels = 1
        )
        WHERE num_single_day_sales = 1
            OR (YEAR(sale_date) > 2020)
    )
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

combined_sales AS (
    SELECT
        COALESCE(uq_sales.pin, md_sales.pin) AS pin_coalesced,
        COALESCE(uq_sales.year, md_sales.year) AS year_coalesced,
        COALESCE(uq_sales.township_code, tc.township_code)
            AS township_code_coalesced,
        COALESCE(uq_sales.nbhd, tc.nbhd) AS nbhd_coalesced,
        COALESCE(uq_sales.class, tc.class) AS class_coalesced,
        CASE
            WHEN uq_sales.year < '2021'
                THEN COALESCE(md_sales.sale_date, uq_sales.sale_date)
            ELSE COALESCE(uq_sales.sale_date, md_sales.sale_date)
        END AS sale_date_coalesced,
        CASE
            WHEN (uq_sales.year < '2021' OR uq_sales.sale_date IS NULL)
                AND md_sales.sale_date IS NOT NULL
                THEN TRUE
            WHEN (uq_sales.year >= '2021' OR md_sales.sale_date IS NULL)
                AND uq_sales.sale_date IS NOT NULL
                THEN FALSE
        END AS is_mydec_date,
        COALESCE(uq_sales.sale_price, md_sales.sale_price)
            AS sale_price_coalesced,
        uq_sales.sale_key,
        COALESCE(uq_sales.doc_no, md_sales.doc_no) AS doc_no_coalesced,
        COALESCE(uq_sales.deed_type, md_sales.mydec_deed_type)
            AS deed_type_coalesced,
        uq_sales.deed_type AS deed_type_ias,
        COALESCE(uq_sales.seller_name, md_sales.seller_name)
            AS seller_name_coalesced,
        COALESCE(uq_sales.is_multisale, md_sales.is_multisale)
            AS is_multisale_coalesced,
        COALESCE(uq_sales.num_parcels_sale, md_sales.num_parcels_sale)
            AS num_parcels_sale_coalesced,
        COALESCE(uq_sales.buyer_name, md_sales.buyer_name)
            AS buyer_name_coalesced,
        COALESCE(uq_sales.sale_type, NULL) AS sale_type_coalesced,
        uq_sales.max_price,
        uq_sales.bad_doc_no,
        CASE WHEN uq_sales.doc_no IS NOT NULL THEN 'iasworld' ELSE 'mydec' END
            AS source,
        md_sales.mydec_deed_type,
        md_sales.sale_filter_ptax_flag,
        md_sales.mydec_property_advertised,
        md_sales.mydec_is_installment_contract_fulfilled,
        md_sales.mydec_is_sale_between_related_individuals_or_corporate_affiliates,
        md_sales.mydec_is_transfer_of_less_than_100_percent_interest,
        md_sales.mydec_is_court_ordered_sale,
        md_sales.mydec_is_sale_in_lieu_of_foreclosure,
        md_sales.mydec_is_condemnation,
        md_sales.mydec_is_short_sale,
        md_sales.mydec_is_bank_reo_real_estate_owned,
        md_sales.mydec_is_auction_sale,
        md_sales.mydec_is_seller_buyer_a_relocation_company,
        md_sales.mydec_is_seller_buyer_a_financial_institution_or_government_agency,
        md_sales.mydec_is_buyer_a_real_estate_investment_trust,
        md_sales.mydec_is_buyer_a_pension_fund,
        md_sales.mydec_is_buyer_an_adjacent_property_owner,
        md_sales.mydec_is_buyer_exercising_an_option_to_purchase,
        md_sales.mydec_is_simultaneous_trade_of_property,
        md_sales.mydec_is_sale_leaseback,
        md_sales.mydec_is_homestead_exemption,
        md_sales.mydec_homestead_exemption_general_alternative,
        md_sales.mydec_homestead_exemption_senior_citizens,
        md_sales.mydec_homestead_exemption_senior_citizens_assessment_freeze,
        -- Include sale_filter_same_sale_within_365 from both sources
        COALESCE(uq_sales.sale_filter_same_sale_within_365, md_sales.sale_filter_same_sale_within_365, FALSE) AS sale_filter_same_sale_within_365,
        -- Include sale_filter_less_than_10k and sale_filter_deed_type
        -- Use appropriate values based on source
        CASE
            WHEN uq_sales.doc_no IS NOT NULL THEN uq_sales.sale_filter_less_than_10k
            ELSE (md_sales.sale_price <= 10000)
        END AS sale_filter_less_than_10k,
        CASE
            WHEN uq_sales.doc_no IS NOT NULL THEN uq_sales.sale_filter_deed_type
            ELSE (
                md_sales.mydec_deed_type IN ('03', '04', '06')
                OR md_sales.mydec_deed_type IS NULL
            )
        END AS sale_filter_deed_type
    FROM unique_sales AS uq_sales
    FULL OUTER JOIN mydec_sales AS md_sales
        ON uq_sales.doc_no = md_sales.doc_no
    LEFT JOIN town_class AS tc
        ON COALESCE(uq_sales.pin, md_sales.pin) = tc.parid
        AND COALESCE(uq_sales.year, md_sales.year) = tc.taxyr
)

SELECT
    cs.pin_coalesced AS pin,
    cs.year_coalesced AS year,
    cs.township_code_coalesced AS township_code,
    cs.nbhd_coalesced AS nbhd,
    cs.class_coalesced AS class,
    cs.sale_date_coalesced AS sale_date,
    cs.is_mydec_date,
    cs.sale_price_coalesced AS sale_price,
    cs.sale_key,
    cs.doc_no_coalesced AS doc_no,
    cs.deed_type_coalesced AS deed_type,
    cs.seller_name_coalesced AS seller_name,
    cs.is_multisale_coalesced AS is_multisale,
    cs.num_parcels_sale_coalesced AS num_parcels_sale,
    cs.buyer_name_coalesced AS buyer_name,
    cs.sale_type_coalesced AS sale_type,
    cs.sale_filter_same_sale_within_365,
    cs.sale_filter_less_than_10k,
    cs.sale_filter_deed_type,
    -- Our sales validation pipeline only validates sales past 2014 due to MyDec
    -- limitations. Previous to that values for sv_is_outlier will be NULL, so
    -- if we want to both exclude detected outliers and include sales prior to
    -- 2014, we need to code everything NULL as FALSE.
    COALESCE(sales_val.sv_is_outlier, FALSE) AS sale_filter_is_outlier,
    cs.mydec_deed_type,
    cs.sale_filter_ptax_flag,
    cs.mydec_property_advertised,
    cs.mydec_is_installment_contract_fulfilled,
    cs.mydec_is_sale_between_related_individuals_or_corporate_affiliates,
    cs.mydec_is_transfer_of_less_than_100_percent_interest,
    cs.mydec_is_court_ordered_sale,
    cs.mydec_is_sale_in_lieu_of_foreclosure,
    cs.mydec_is_condemnation,
    cs.mydec_is_short_sale,
    cs.mydec_is_bank_reo_real_estate_owned,
    cs.mydec_is_auction_sale,
    cs.mydec_is_seller_buyer_a_relocation_company,
    cs.mydec_is_seller_buyer_a_financial_institution_or_government_agency,
    cs.mydec_is_buyer_a_real_estate_investment_trust,
    cs.mydec_is_buyer_a_pension_fund,
    cs.mydec_is_buyer_an_adjacent_property_owner,
    cs.mydec_is_buyer_exercising_an_option_to_purchase,
    cs.mydec_is_simultaneous_trade_of_property,
    cs.mydec_is_sale_leaseback,
    cs.mydec_is_homestead_exemption,
    cs.mydec_homestead_exemption_general_alternative,
    cs.mydec_homestead_exemption_senior_citizens,
    cs.mydec_homestead_exemption_senior_citizens_assessment_freeze,
    sales_val.sv_is_outlier,
    sales_val.sv_is_ptax_outlier,
    sales_val.sv_is_heuristic_outlier,
    sales_val.sv_outlier_reason1,
    sales_val.sv_outlier_reason2,
    sales_val.sv_outlier_reason3,
    sales_val.sv_run_id,
    sales_val.sv_version,
    cs.source
FROM combined_sales AS cs
LEFT JOIN sales_val
    ON cs.doc_no_coalesced = sales_val.meta_sale_document_num;
