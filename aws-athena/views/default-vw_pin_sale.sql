-- View containing unique, filtered sales
CREATE OR REPLACE VIEW default.vw_pin_sale AS
-- Class and township of associated PIN
WITH town_class AS (
    SELECT
        par.parid,
        par.class,
        par.taxyr,
        leg.user1 AS township_code,
        CONCAT(leg.user1, SUBSTR(par.nbhd, 3, 3)) AS nbhd
    FROM iasworld.pardat AS par
    LEFT JOIN iasworld.legdat AS leg
        ON par.parid = leg.parid
        AND par.taxyr = leg.taxyr
),

-- "nopar" isn't entirely accurate for sales associated with only one parcel,
-- so we create our own counter
calculated AS (
    SELECT
        instruno,
        COUNT(*) AS nopar_calculated
    FROM iasworld.sales
    WHERE deactivat IS NULL
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
            -- and give the top observation a value of 1 for "max_price"
            ROW_NUMBER() OVER (
                PARTITION BY sales.parid, sales.saledt ORDER BY sales.price DESC
            ) AS max_price,
            -- Some pins sell for the exact same price a few months after
            -- they're sold. These sales are unecessary for modeling and may be
            -- duplicates
            LAG(DATE_PARSE(SUBSTR(sales.saledt, 1, 10), '%Y-%m-%d')) OVER (
                PARTITION BY sales.parid, sales.price ORDER BY sales.saledt
            ) AS same_price_earlier_date
        FROM iasworld.sales
        LEFT JOIN calculated
            ON sales.instruno = calculated.instruno
        LEFT JOIN
            town_class AS tc
            ON sales.parid = tc.parid
            AND SUBSTR(sales.saledt, 1, 4) = tc.taxyr

        WHERE sales.instruno IS NOT NULL
        -- Indicates whether a record has been deactivated
            AND sales.deactivat IS NULL
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
    -- Drop sales for a given pin if it has sold within the last 12 months
    -- for the same price
        AND (
            EXTRACT(DAY FROM sale_date - same_price_earlier_date) > 365
            OR same_price_earlier_date IS NULL
        )
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
            -- reporting.
            (
                COALESCE(line_10b, 0) + COALESCE(line_10c, 0)
                + COALESCE(line_10d, 0) + COALESCE(line_10e, 0)
                + COALESCE(line_10f, 0) + COALESCE(line_10g, 0)
                + COALESCE(line_10h, 0) + COALESCE(line_10i, 0)
                + COALESCE(line_10k, 0)
            ) > 0 AS sale_filter_ptax_flag,
            COUNT() OVER (
                PARTITION BY line_1_primary_pin, line_4_instrument_date
            ) AS num_cards_sale
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
)

SELECT
    unique_sales.pin,
    unique_sales.year,
    unique_sales.township_code,
    unique_sales.nbhd,
    unique_sales.class,
    --- In the past, mydec sale dates were more precise than iasworld dates
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
    mydec_sales.sale_filter_ptax_flag,
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
    mydec_sales.homestead_exemption_senior_citizens_assessment_freeze
FROM unique_sales
LEFT JOIN mydec_sales
    ON unique_sales.doc_no = mydec_sales.doc_no
    AND unique_sales.pin = mydec_sales.pin
