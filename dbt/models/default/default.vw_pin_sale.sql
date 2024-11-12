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
            DATE_DIFF(
                'day',
                same_price_earlier_date,
                sale_date
            ) <= 365,
            FALSE
        ) AS sale_filter_same_sale_within_365,
        -- Compute sale_filter_same_iasworld_sale_within_365 using the same logic --noqa
        COALESCE(
            DATE_DIFF(
                'day',
                same_price_earlier_date,
                sale_date
            ) <= 365,
            FALSE
        ) AS sale_filter_same_iasworld_sale_within_365 --noqa
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
            -- "nopar" is number of parcels sold
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
    SELECT * FROM (
        SELECT
            REPLACE(document_number, 'D', '') AS doc_no,
            REPLACE(line_1_primary_pin, '-', '') AS pin,
            DATE_PARSE(line_4_instrument_date, '%Y-%m-%d') AS sale_date,
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
            COALESCE(line_10b = 1, FALSE) --noqa
                AS mydec_is_sale_between_related_individuals_or_corporate_affiliates, --noqa
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
                AS mydec_is_seller_buyer_a_financial_institution_or_government_agency, --noqa
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
            ) AS num_single_day_sales,
            year_of_sale AS year
        FROM {{ source('sale', 'mydec') }}
        --WHERE line_2_total_parcels < 2
    )
    /* Some sales in mydec have multiple rows for one pin on a given sale date.
    Sometimes they have different dates than iasworld prior to 2021 and when
    joined back onto unique_sales will create duplicates by pin/sale date. */
    WHERE num_single_day_sales = 1
        OR year > '2020'
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

-- CTE to coalesce iasworld and mydec values prior to
-- constructing filters that depend on coalesced fields
combined_sales AS (
    SELECT
        COALESCE(uq_sales.pin, md_sales.pin) AS pin_coalesced,
        -- For many of the fields we used  simple coalesce statement,
        -- but some data is a bit more complicated. Prior to 2021,
        -- mydec sales and iasworld sales used different sale dates.
        -- We preference the mydec sale as they are believed to be more
        -- accurate. As of 2021, iasworld utilizes mydec sales, which means
        -- we can prioritize iasworld data instead of mydec data.
        CASE
            WHEN uq_sales.year < '2021'
                THEN COALESCE(md_sales.year, uq_sales.year)
            ELSE COALESCE(uq_sales.year, md_sales.year)
        END AS year_coalesced,
        COALESCE(uq_sales.township_code, tc.township_code)
            AS township_code_coalesced, --noqa
        COALESCE(uq_sales.nbhd, tc.nbhd) AS nbhd_coalesced,
        COALESCE(uq_sales.class, tc.class) AS class_coalesced,
        CASE --noqa
            WHEN
                uq_sales.year < '2021'
                THEN COALESCE(md_sales.sale_date, uq_sales.sale_date)
            ELSE COALESCE(uq_sales.sale_date, md_sales.sale_date)
        END AS sale_date_coalesced,
        COALESCE(
            md_sales.sale_date IS NOT NULL
            OR YEAR(uq_sales.sale_date) >= 2021,
            FALSE
        ) AS is_mydec_date,
        COALESCE(uq_sales.sale_filter_same_iasworld_sale_within_365, FALSE)
            AS sale_filter_same_iasworld_sale_within_365,
        COALESCE(uq_sales.sale_price, md_sales.sale_price)
            AS sale_price_coalesced, --noqa
        uq_sales.sale_key,
        COALESCE(uq_sales.doc_no, md_sales.doc_no) AS doc_no_coalesced, --noqa
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
        md_sales.mydec_is_sale_between_related_individuals_or_corporate_affiliates, --noqa
        md_sales.mydec_is_transfer_of_less_than_100_percent_interest,
        md_sales.mydec_is_court_ordered_sale,
        md_sales.mydec_is_sale_in_lieu_of_foreclosure,
        md_sales.mydec_is_condemnation,
        md_sales.mydec_is_short_sale,
        md_sales.mydec_is_bank_reo_real_estate_owned,
        md_sales.mydec_is_auction_sale,
        md_sales.mydec_is_seller_buyer_a_relocation_company,
        md_sales.mydec_is_seller_buyer_a_financial_institution_or_government_agency, --noqa
        md_sales.mydec_is_buyer_a_real_estate_investment_trust,
        md_sales.mydec_is_buyer_a_pension_fund,
        md_sales.mydec_is_buyer_an_adjacent_property_owner,
        md_sales.mydec_is_buyer_exercising_an_option_to_purchase,
        md_sales.mydec_is_simultaneous_trade_of_property,
        md_sales.mydec_is_sale_leaseback,
        md_sales.mydec_is_homestead_exemption,
        md_sales.mydec_homestead_exemption_general_alternative,
        md_sales.mydec_homestead_exemption_senior_citizens,
        md_sales.mydec_homestead_exemption_senior_citizens_assessment_freeze
    FROM unique_sales AS uq_sales
    -- This logic brings in mydec sales that aren't in iasworld.
    -- If a doc_no exists in iasworld and mydec, we prioritize iasworld,
    -- if it only exists in mydec, we will grab the doc_no from mydec. The
    -- 'source' column lets us know which table the doc_no came from and allows
    -- us to filter for only iasworld sales or for mydec sales that aren't in
    -- iasworld already.
    FULL OUTER JOIN mydec_sales AS md_sales
        ON uq_sales.doc_no = md_sales.doc_no
        AND md_sales.is_multisale = FALSE
    LEFT JOIN town_class AS tc
        ON COALESCE(uq_sales.pin, md_sales.pin) = tc.parid
        AND COALESCE(uq_sales.year, md_sales.year) = tc.taxyr
),

-- Handle various filters 
add_filter_sales AS (
    SELECT
        cs.*,
        -- Calculate 'sale_filter_same_sale_within_365' using DATE_DIFF
        -- Note: the sale_filter_same_sale_within_365 uses both iasworld
        -- and mydec doc numbers for the calculation. So if we were to set
        -- source = 'iasworld', mydec sales will still influence this filter
        CASE
            WHEN LAG(cs.sale_date_coalesced) OVER (
                    PARTITION BY cs.pin_coalesced, cs.sale_price_coalesced
                    ORDER BY cs.sale_date_coalesced ASC
                ) IS NOT NULL
                THEN
                DATE_DIFF(
                    'day',
                    LAG(cs.sale_date_coalesced) OVER (
                        PARTITION BY
                            cs.pin_coalesced, cs.sale_price_coalesced
                        ORDER BY cs.sale_date_coalesced ASC
                    ),
                    cs.sale_date_coalesced
                ) <= 365
            ELSE FALSE
        END AS sale_filter_same_sale_within_365,
        -- Compute 'sale_filter_less_than_10k'
        (cs.sale_price_coalesced <= 10000) AS sale_filter_less_than_10k,
        -- Compute 'sale_filter_deed_type'
        (
            cs.deed_type_coalesced IN ('03', '04', '06')
            OR cs.deed_type_coalesced IS NULL
        ) AS sale_filter_deed_type
    FROM combined_sales AS cs
)

SELECT
    afs.pin_coalesced AS pin,
    afs.year_coalesced AS year,
    afs.township_code_coalesced AS township_code,
    afs.nbhd_coalesced AS nbhd,
    afs.class_coalesced AS class,
    afs.sale_date_coalesced AS sale_date,
    afs.is_mydec_date,
    afs.sale_price_coalesced AS sale_price,
    afs.sale_key,
    afs.doc_no_coalesced AS doc_no,
    afs.deed_type_coalesced AS deed_type,
    afs.seller_name_coalesced AS seller_name,
    afs.is_multisale_coalesced AS is_multisale,
    afs.num_parcels_sale_coalesced AS num_parcels_sale,
    afs.buyer_name_coalesced AS buyer_name,
    afs.sale_type_coalesced AS sale_type,
    afs.sale_filter_same_sale_within_365,
    afs.sale_filter_same_iasworld_sale_within_365,
    afs.sale_filter_less_than_10k,
    afs.sale_filter_deed_type,
    -- Our sales validation pipeline only validates sales past 2014 due to MyDec
    -- limitations. Previous to that values for sv_is_outlier will be NULL, so
    -- if we want to both exclude detected outliers and include ssales prior to
    -- 2014, we need to code everything NULL as FALSE.
    COALESCE(sales_val.sv_is_outlier, FALSE) AS sale_filter_is_outlier,
    afs.mydec_deed_type,
    afs.sale_filter_ptax_flag,
    afs.mydec_property_advertised,
    afs.mydec_is_installment_contract_fulfilled,
    afs.mydec_is_sale_between_related_individuals_or_corporate_affiliates, --noqa
    afs.mydec_is_transfer_of_less_than_100_percent_interest,
    afs.mydec_is_court_ordered_sale,
    afs.mydec_is_sale_in_lieu_of_foreclosure,
    afs.mydec_is_condemnation,
    afs.mydec_is_short_sale,
    afs.mydec_is_bank_reo_real_estate_owned,
    afs.mydec_is_auction_sale,
    afs.mydec_is_seller_buyer_a_relocation_company,
    afs.mydec_is_seller_buyer_a_financial_institution_or_government_agency, --noqa
    afs.mydec_is_buyer_a_real_estate_investment_trust,
    afs.mydec_is_buyer_a_pension_fund,
    afs.mydec_is_buyer_an_adjacent_property_owner,
    afs.mydec_is_buyer_exercising_an_option_to_purchase,
    afs.mydec_is_simultaneous_trade_of_property,
    afs.mydec_is_sale_leaseback,
    afs.mydec_is_homestead_exemption,
    afs.mydec_homestead_exemption_general_alternative,
    afs.mydec_homestead_exemption_senior_citizens,
    afs.mydec_homestead_exemption_senior_citizens_assessment_freeze,
    sales_val.sv_is_outlier,
    sales_val.sv_is_ptax_outlier,
    sales_val.sv_is_heuristic_outlier,
    sales_val.sv_outlier_reason1,
    sales_val.sv_outlier_reason2,
    sales_val.sv_outlier_reason3,
    sales_val.sv_run_id,
    sales_val.sv_version,
    afs.source
FROM add_filter_sales AS afs
LEFT JOIN sales_val
    ON afs.doc_no_coalesced = sales_val.meta_sale_document_num;
