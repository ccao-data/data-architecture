-- View containing unique, filtered sales
CREATE OR replace VIEW default.vw_pin_sale
AS
-- Township and class of associated PIN
WITH townclass AS (
    SELECT DISTINCT
        parid,
        class,
        Substr(nbhd, 1, 2) AS township_code,
        taxyr
    FROM iasworld.pardat),
-- "nopar" isn't entirely accurate for sales associated with only one parcel, so we create our own counter
calculated AS (

    SELECT
        instruno,
        Count(*) AS nopar_calculated
    FROM iasworld.sales
    WHERE deactivat IS NULL
    GROUP BY instruno

    ),
unique_sales AS (
    SELECT *
    FROM (

        SELECT
            sales.parid AS pin,
            Substr(sales.saledt, 1, 4) AS year,
            townclass.township_code,
            townclass.class,
            Date_parse(Substr(sales.saledt, 1, 10), '%Y-%m-%d') AS sale_date,
            Cast(sales.price AS BIGINT) AS sale_price,
            Log(sales.price, 10) AS sale_price_log10,
            sales.salekey AS sale_key,
            Nullif(replace(sales.instruno, 'D', ''), '') AS doc_no,
            Nullif(sales.instrtyp, '') AS deed_type,
            -- "nopar" is number of parcels sold
            case when sales.nopar <= 1 AND calculated.nopar_calculated = 1 then FALSE else TRUE end as is_multisale,
            case when sales.nopar > 1 then sales.nopar else calculated.nopar_calculated end as num_parcels_sale,
            Nullif(sales.oldown, '') AS seller_name,
            Nullif(sales.own1, '') AS buyer_name,
            CASE
                WHEN sales.saletype = '0' THEN 'LAND'
                WHEN sales.saletype = '1' THEN 'LAND AND BUILDING'
            END AS sale_type,
            -- Sales are not entirely unique by pin/date so we group all sales b pin/date
            -- then order then order by descending price and give the top observation a value of 1 for "max_price"
            Row_number() OVER(
                PARTITION BY sales.parid, sales.saledt ORDER BY -1 * sales.price
                ) AS max_price,
           -- Some pins sell for the exact same price a few months after they're sold
           -- these sales are unecessary for modeling and may be duplicates
        Lag(Date_parse(Substr(sales.saledt, 1, 10), '%Y-%m-%d')) OVER(
            PARTITION BY sales.parid, sales.price ORDER BY sales.saledt
            ) AS same_price_earlier_date
    FROM iasworld.sales
    LEFT JOIN calculated ON sales.instruno = calculated.instruno
    LEFT JOIN townclass
        ON sales.parid = townclass.parid
        AND Substr(sales.saledt, 1, 4) = townclass.taxyr
    WHERE sales.instruno IS NOT NULL
        -- Indicates whether a record has been deactivated
        AND sales.deactivat IS NULL
        AND sales.price > 10000
        AND Cast(Substr(sales.saledt, 1, 4) AS INT) BETWEEN 1997 AND Year(current_date)
        -- Exclude quit claims, executor deeds, beneficial interests
        AND instrtyp NOT IN ( '03', '04', '06' )
        AND townclass.township_code IS NOT NULL

        )

    -- Only use max price by pin/sale date
    WHERE  max_price = 1
    -- Drop sales for a given pin if it has sold within the last 12 months for the same price
    AND (
        Extract(day FROM sale_date - same_price_earlier_date) > 365
        OR same_price_earlier_date IS NULL
    )

),
-- Lower and upper bounds so that outlier sales can be filtered out
sale_filter AS (
    SELECT
        township_code,
        class,
        year,
        is_multisale,
        Avg(sale_price_log10) - STDDEV(sale_price_log10) * 3.6 AS sale_filter_lower_limit,
        Avg(sale_price_log10) + STDDEV(sale_price_log10) * 3.6 AS sale_filter_upper_limit,
        Count(*) AS sale_filter_count
    FROM unique_sales
    GROUP BY
        township_code,
        class,
        year,
        is_multisale
        ),
mydec_sales AS (

    SELECT
        replace(document_number, 'D', '') as doc_no,
        replace(line_1_primary_pin, '-', '') as pin,
        Date_parse(line_4_instrument_date, '%Y-%m-%d') AS mydec_date,
        CASE WHEN line_7_property_advertised = 1 THEN TRUE ELSE FALSE END AS "property_advertised",
        CASE WHEN line_10a = 1 THEN TRUE ELSE FALSE END AS "is_installment_contract_fulfilled",
        CASE WHEN line_10b = 1 THEN TRUE ELSE FALSE END AS "is_sale_between_related_individuals_or_corporate_affiliates",
        CASE WHEN line_10c = 1 THEN TRUE ELSE FALSE END AS "is_transfer_of_less_than_100_percent_interest",
        CASE WHEN line_10d = 1 THEN TRUE ELSE FALSE END AS "is_court_ordered_sale",
        CASE WHEN line_10e = 1 THEN TRUE ELSE FALSE END AS "is_sale_in_lieu_of_foreclosure",
        CASE WHEN line_10f = 1 THEN TRUE ELSE FALSE END AS "is_condemnation",
        CASE WHEN line_10g = 1 THEN TRUE ELSE FALSE END AS "is_short_sale",
        CASE WHEN line_10h = 1 THEN TRUE ELSE FALSE END AS "is_bank_reo_real_estate_owned",
        CASE WHEN line_10i = 1 THEN TRUE ELSE FALSE END AS "is_auction_sale",
        CASE WHEN line_10j = 1 THEN TRUE ELSE FALSE END AS "is_seller_buyer_a_relocation_company",
        CASE WHEN line_10k = 1 THEN TRUE ELSE FALSE END AS "is_seller_buyer_a_financial_institution_or_government_agency",
        CASE WHEN line_10l = 1 THEN TRUE ELSE FALSE END AS "is_buyer_a_real_estate_investment_trust",
        CASE WHEN line_10m = 1 THEN TRUE ELSE FALSE END AS "is_buyer_a_pension_fund",
        CASE WHEN line_10n = 1 THEN TRUE ELSE FALSE END AS "is_buyer_an_adjacent_property_owner",
        CASE WHEN line_10o = 1 THEN TRUE ELSE FALSE END AS "is_buyer_exercising_an_option_to_purchase",
        CASE WHEN line_10p = 1 THEN TRUE ELSE FALSE END AS "is_simultaneous_trade_of_property",
        CASE WHEN line_10q = 1 THEN TRUE ELSE FALSE END AS "is_sale_leaseback",
        CASE WHEN line_10s = 1 THEN TRUE ELSE FALSE END AS "is_homestead_exemption",
        line_10s_generalalternative AS "homestead_exemption_general_alternative",
        line_10s_senior_citizens AS "homestead_exemption_senior_citizens",
        line_10s_senior_citizens_assessment_freeze AS "homestead_exemption_senior_citizens_assessment_freeze"
    FROM sale.mydec
    WHERE is_earliest_within_doc_no = TRUE

)

SELECT
    unique_sales.pin,
    unique_sales.year,
    unique_sales.township_code,
    unique_sales.class,
    --- In the past, mydec sale dates are more precise than iasworld dates which had been truncated
    CASE
        WHEN mydec_date IS NOT NULL AND mydec_date != unique_sales.sale_date THEN mydec_date
        ELSE unique_sales.sale_date END AS sale_date,
    -- From 2021 on iasWorld uses precise MyDec dates
    CASE
        WHEN mydec_date IS NOT NULL OR YEAR(unique_sales.sale_date) >= 2021 THEN TRUE
        ELSE FALSE END AS is_mydec_date,
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
    sale_filter_lower_limit,
    sale_filter_upper_limit,
    sale_filter_count,
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
LEFT JOIN sale_filter
    ON unique_sales.township_code = sale_filter.township_code
    AND unique_sales.class = sale_filter.class
    AND unique_sales.year = sale_filter.year
    AND unique_sales.is_multisale = sale_filter.is_multisale
LEFT JOIN mydec_sales
    ON unique_sales.doc_no = mydec_sales.doc_no
    AND unique_sales.pin = mydec_sales.pin