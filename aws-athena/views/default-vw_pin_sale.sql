-- View containing unique, filtered sales
-- Class and township of associated PIN
WITH town_class AS (
    SELECT
        par.parid,
        par.class,
        par.taxyr,
        leg.user1 AS township_code,
        CONCAT(leg.user1, SUBSTR(par.nbhd, 3, 3)) AS nbhd
    FROM {{ source('iasworld', 'pardat') }} AS par
    LEFT JOIN {{ source('iasworld', 'legdat') }} AS leg
        ON par.parid = leg.parid
        AND par.taxyr = leg.taxyr
    WHERE par.cur = 'Y'
        AND par.deactivat IS NULL
        AND leg.cur = 'Y'
        AND leg.deactivat IS NULL
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
            instruno
        FROM {{ source('iasworld', 'sales') }}
        WHERE deactivat IS NULL
            AND cur = 'Y'
    )
    GROUP BY instruno
),

unique_sales AS (
    SELECT *
    FROM (
        SELECT
            sales.parid AS pin,
            SUBSTR(sales.saledt, 1, 4) AS year,
            tc.township_code,
            tc.nbhd,
            tc.class,
            DATE_PARSE(SUBSTR(sales.saledt, 1, 10), '%Y-%m-%d') AS sale_date,
            CAST(sales.price AS BIGINT) AS sale_price,
            LOG10(sales.price) AS sale_price_log10,
            sales.salekey AS sale_key,
            NULLIF(REPLACE(sales.instruno, 'D', ''), '') AS doc_no,
            NULLIF(sales.instrtyp, '') AS deed_type,
            -- "nopar" is number of parcels sold
            NOT COALESCE(
                sales.nopar <= 1 AND calculated.nopar_calculated = 1,
                FALSE
            ) AS is_multisale,
            CASE
                WHEN sales.nopar > 1 THEN sales.nopar ELSE
                    calculated.nopar_calculated
            END AS num_parcels_sale,
            NULLIF(sales.oldown, '') AS seller_name,
            NULLIF(sales.own1, '') AS buyer_name,
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
                PARTITION BY sales.parid, sales.saledt
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
                PARTITION BY NULLIF(REPLACE(sales.instruno, 'D', ''), '')
                ORDER BY sales.saledt
            ) AS bad_doc_no,
            -- Some pins sell for the exact same price a few months after
            -- they're sold. These sales are unecessary for modeling and may be
            -- duplicates.
            -- We need to order by salekey as well in case of any ties within
            -- price, date, and pin.
            LAG(DATE_PARSE(SUBSTR(sales.saledt, 1, 10), '%Y-%m-%d')) OVER (
                PARTITION BY sales.parid, sales.price
                ORDER BY sales.saledt ASC, sales.salekey ASC
            ) AS same_price_earlier_date
        FROM {{ source('iasworld', 'sales') }} AS sales
        LEFT JOIN calculated
            ON sales.instruno = calculated.instruno
        LEFT JOIN
            town_class AS tc
            ON sales.parid = tc.parid
            AND SUBSTR(sales.saledt, 1, 4) = tc.taxyr

        WHERE sales.instruno IS NOT NULL
        -- Indicates whether a record has been deactivated
            AND sales.deactivat IS NULL
            AND sales.cur = 'Y'
            AND sales.price > 10000
            AND CAST(SUBSTR(sales.saledt, 1, 4) AS INT) BETWEEN 1997 AND YEAR(
                CURRENT_DATE
            )
            -- Exclude quit claims, executor deeds, beneficial interests
            AND sales.instrtyp NOT IN ('03', '04', '06')
            AND tc.township_code IS NOT NULL
    )
    -- Only use max price by pin/sale date
    WHERE max_price = 1
        AND (bad_doc_no = 1 OR is_multisale = TRUE)
        -- Drop sales for a given pin if it has sold within the last 12 months
        -- for the same price
        AND (
            EXTRACT(DAY FROM sale_date - same_price_earlier_date) > 365
            OR same_price_earlier_date IS NULL
        )
),

-- Lower and upper bounds so that outlier sales can be filtered
-- out using PTAX-203 data
sale_filter AS (
    SELECT
        township_code,
        class,
        year,
        is_multisale,
        AVG(sale_price_log10)
        - STDDEV(sale_price_log10) * 2 AS sale_filter_lower_limit,
        AVG(sale_price_log10)
        + STDDEV(sale_price_log10) * 2 AS sale_filter_upper_limit,
        COUNT(*) AS sale_filter_count
    FROM unique_sales
    GROUP BY
        township_code,
        class,
        year,
        is_multisale
),

mydec_sales AS (
    SELECT * FROM (
        SELECT
            REPLACE(document_number, 'D', '') AS doc_no,
            REPLACE(line_1_primary_pin, '-', '') AS pin,
            DATE_PARSE(line_4_instrument_date, '%Y-%m-%d') AS mydec_date,
            COALESCE(line_7_property_advertised = 1, FALSE)
                AS property_advertised,
            COALESCE(line_10a = 1, FALSE)
                AS is_installment_contract_fulfilled,
            COALESCE(line_10b = 1, FALSE)
                AS is_sale_between_related_individuals_or_corporate_affiliates,
            COALESCE(line_10c = 1, FALSE)
                AS is_transfer_of_less_than_100_percent_interest,
            COALESCE(line_10d = 1, FALSE)
                AS is_court_ordered_sale,
            COALESCE(line_10e = 1, FALSE)
                AS is_sale_in_lieu_of_foreclosure,
            COALESCE(line_10f = 1, FALSE)
                AS is_condemnation,
            COALESCE(line_10g = 1, FALSE)
                AS is_short_sale,
            COALESCE(line_10h = 1, FALSE)
                AS is_bank_reo_real_estate_owned,
            COALESCE(line_10i = 1, FALSE)
                AS is_auction_sale,
            COALESCE(line_10j = 1, FALSE)
                AS is_seller_buyer_a_relocation_company,
            COALESCE(line_10k = 1, FALSE)
                AS is_seller_buyer_a_financial_institution_or_government_agency,
            COALESCE(line_10l = 1, FALSE)
                AS is_buyer_a_real_estate_investment_trust,
            COALESCE(line_10m = 1, FALSE)
                AS is_buyer_a_pension_fund,
            COALESCE(line_10n = 1, FALSE)
                AS is_buyer_an_adjacent_property_owner,
            COALESCE(line_10o = 1, FALSE)
                AS is_buyer_exercising_an_option_to_purchase,
            COALESCE(line_10p = 1, FALSE)
                AS is_simultaneous_trade_of_property,
            COALESCE(line_10q = 1, FALSE)
                AS is_sale_leaseback,
            COALESCE(line_10s = 1, FALSE)
                AS is_homestead_exemption,
            line_10s_generalalternative
                AS homestead_exemption_general_alternative,
            line_10s_senior_citizens
                AS homestead_exemption_senior_citizens,
            line_10s_senior_citizens_assessment_freeze
                AS homestead_exemption_senior_citizens_assessment_freeze,
            -- Flag for booting outlier PTAX-203 sales from modeling and
            -- reporting. Used in combination with sale_filter upper and lower,
            -- which finds sales more than 2 SD from the year, town, and
            -- class mean
            (
                COALESCE(line_10b, 0) + COALESCE(line_10c, 0)
                + COALESCE(line_10d, 0) + COALESCE(line_10e, 0)
                + COALESCE(line_10f, 0) + COALESCE(line_10g, 0)
                + COALESCE(line_10h, 0) + COALESCE(line_10i, 0)
                + COALESCE(line_10k, 0)
            ) > 0 AS sale_filter_ptax_flag,
            COUNT() OVER (
                PARTITION BY line_1_primary_pin, line_4_instrument_date
            ) AS num_cards_sale,
            year_of_sale
        FROM sale.mydec
        WHERE is_earliest_within_doc_no
    )
    /* Some sales in mydec have multiple rows for one pin on a given sale date.
    These are likely individual cards being sold since they have different doc
    numbers. The issue is that sometimes they have different dates than iasworld
    prior to 2021 and when joined back onto unique_sales will create duplicates
    by pin/sale date. We don't know whether these values should be summed or
    not, so we'll exclude them to avoid afore mentioned duplicates. */
    WHERE num_cards_sale = 1
        OR (YEAR(mydec_date) > 2020)
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
        sf.sv_outlier_type,
        sf.run_id AS sv_run_id,
        sf.version AS sv_version
    FROM {{ source('sale', 'flag') }} AS sf
    INNER JOIN max_version_flag AS mv
        ON sf.meta_sale_document_num = mv.meta_sale_document_num
        AND sf.version = mv.max_version
)

SELECT
    unique_sales.pin,
    -- In the past, mydec sale dates were more precise than iasworld dates
    -- which had been truncated
    CASE
        WHEN
            mydec_sales.mydec_date IS NOT NULL
            AND mydec_sales.mydec_date != unique_sales.sale_date
            THEN mydec_sales.year_of_sale
        ELSE unique_sales.year
    END AS year,
    unique_sales.township_code,
    unique_sales.nbhd,
    unique_sales.class,
    -- In the past, mydec sale dates were more precise than iasworld dates
    -- which had been truncated
    CASE
        WHEN
            mydec_sales.mydec_date IS NOT NULL
            AND mydec_sales.mydec_date != unique_sales.sale_date
            THEN mydec_sales.mydec_date
        ELSE unique_sales.sale_date
    END AS sale_date,
    -- From 2021 on iasWorld uses precise MyDec dates
    COALESCE(
        mydec_sales.mydec_date IS NOT NULL
        OR YEAR(unique_sales.sale_date) >= 2021,
        FALSE
    ) AS is_mydec_date,
    unique_sales.sale_price,
    unique_sales.sale_price_log10,
    unique_sales.sale_key,
    unique_sales.doc_no,
    unique_sales.deed_type,
    unique_sales.seller_name,
    unique_sales.is_multisale,
    unique_sales.num_parcels_sale,
    unique_sales.buyer_name,
    unique_sales.sale_type,
    sale_filter.sale_filter_lower_limit,
    sale_filter.sale_filter_upper_limit,
    sale_filter.sale_filter_count,
    mydec_sales.sale_filter_ptax_flag,
    COALESCE((
        mydec_sales.sale_filter_ptax_flag
        AND unique_sales.sale_price_log10
        NOT BETWEEN sale_filter.sale_filter_lower_limit
        AND sale_filter.sale_filter_upper_limit
    ), FALSE) AS sale_filter_is_outlier,
    mydec_sales.property_advertised,
    mydec_sales.is_installment_contract_fulfilled,
    mydec_sales.is_sale_between_related_individuals_or_corporate_affiliates,
    mydec_sales.is_transfer_of_less_than_100_percent_interest,
    mydec_sales.is_court_ordered_sale,
    mydec_sales.is_sale_in_lieu_of_foreclosure,
    mydec_sales.is_condemnation,
    mydec_sales.is_short_sale,
    mydec_sales.is_bank_reo_real_estate_owned,
    mydec_sales.is_auction_sale,
    mydec_sales.is_seller_buyer_a_relocation_company,
    mydec_sales.is_seller_buyer_a_financial_institution_or_government_agency,
    mydec_sales.is_buyer_a_real_estate_investment_trust,
    mydec_sales.is_buyer_a_pension_fund,
    mydec_sales.is_buyer_an_adjacent_property_owner,
    mydec_sales.is_buyer_exercising_an_option_to_purchase,
    mydec_sales.is_simultaneous_trade_of_property,
    mydec_sales.is_sale_leaseback,
    mydec_sales.is_homestead_exemption,
    mydec_sales.homestead_exemption_general_alternative,
    mydec_sales.homestead_exemption_senior_citizens,
    mydec_sales.homestead_exemption_senior_citizens_assessment_freeze,
    sales_val.*
FROM unique_sales
LEFT JOIN sale_filter
    ON unique_sales.township_code = sale_filter.township_code
    AND unique_sales.class = sale_filter.class
    AND unique_sales.year = sale_filter.year
    AND unique_sales.is_multisale = sale_filter.is_multisale
LEFT JOIN mydec_sales
    ON unique_sales.doc_no = mydec_sales.doc_no
    AND unique_sales.pin = mydec_sales.pin
LEFT JOIN sales_val
    ON unique_sales.doc_no = sales_val.meta_sale_document_num;
