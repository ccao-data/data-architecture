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

-- Move mydec_sales before unique_sales
mydec_sales AS (
    SELECT * FROM (
        SELECT
            REPLACE(document_number, 'D', '') AS doc_no,
            REPLACE(line_1_primary_pin, '-', '') AS pin,
            DATE_PARSE(line_4_instrument_date, '%Y-%m-%d') AS mydec_date,
            line_5_instrument_type AS mydec_deed_type,
            NULLIF(TRIM(seller_name), '') AS seller_name,
            NULLIF(TRIM(buyer_name), '') AS buyer_name,
            COALESCE(line_7_property_advertised = 1, FALSE)
                AS mydec_property_advertised,
            COALESCE(line_10a = 1, FALSE)
                AS mydec_is_installment_contract_fulfilled,
            COALESCE(line_10b = 1, FALSE)
                AS mydec_is_sale_between_related_individuals_or_corporate_affiliates, -- noqa
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
                AS mydec_is_seller_buyer_a_financial_institution_or_government_agency, -- noqa
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
            ) AS num_single_day_sales,
            year_of_sale
        FROM {{ source('sale', 'mydec') }}
        WHERE line_2_total_parcels = 1 -- Remove multisales
    )
    /* Some sales in mydec have multiple rows for one pin on a given sale date.
    Sometimes they have different dates than iasworld prior to 2021 and when
    joined back onto unique_sales will create duplicates by pin/sale date. */
    WHERE num_single_day_sales = 1
        OR (YEAR(mydec_date) > 2020)
),

unique_sales AS (
    SELECT
        sales.parid AS pin,
        SUBSTR(sales.saledt, 1, 4) AS year,
        tc.township_code,
        tc.nbhd,
        tc.class,
        -- Adjust sale_date using CASE logic
        CASE
            WHEN
                mydec_sales.mydec_date IS NOT NULL
                AND mydec_sales.mydec_date != DATE_PARSE(SUBSTR(sales.saledt, 1, 10), '%Y-%m-%d')
                THEN mydec_sales.mydec_date
            ELSE DATE_PARSE(SUBSTR(sales.saledt, 1, 10), '%Y-%m-%d')
        END AS sale_date,
        CAST(sales.price AS BIGINT) AS sale_price,
        sales.salekey AS sale_key,
        NULLIF(REPLACE(sales.instruno, 'D', ''), '') AS doc_no,
        NULLIF(sales.instrtyp, '') AS deed_type,
        -- [Rest of your existing columns in unique_sales]
        
        -- Recompute same_price_earlier_date using adjusted sale_date
        LAG(
            CASE
                WHEN
                    mydec_sales.mydec_date IS NOT NULL
                    AND mydec_sales.mydec_date != DATE_PARSE(SUBSTR(sales.saledt, 1, 10), '%Y-%m-%d')
                    THEN mydec_sales.mydec_date
                ELSE DATE_PARSE(SUBSTR(sales.saledt, 1, 10), '%Y-%m-%d')
            END
        ) OVER (
            PARTITION BY
                sales.parid,
                sales.price,
                sales.instrtyp NOT IN ('03', '04', '06')
            ORDER BY
                CASE
                    WHEN
                        mydec_sales.mydec_date IS NOT NULL
                        AND mydec_sales.mydec_date != DATE_PARSE(SUBSTR(sales.saledt, 1, 10), '%Y-%m-%d')
                        THEN mydec_sales.mydec_date
                    ELSE DATE_PARSE(SUBSTR(sales.saledt, 1, 10), '%Y-%m-%d')
                END ASC,
                sales.salekey ASC
        ) AS same_price_earlier_date,
        
        -- Recalculate sale_filter_same_sale_within_365 using adjusted dates
        COALESCE(
            EXTRACT(DAY FROM (
                CASE
                    WHEN
                        mydec_sales.mydec_date IS NOT NULL
                        AND mydec_sales.mydec_date != DATE_PARSE(SUBSTR(sales.saledt, 1, 10), '%Y-%m-%d')
                        THEN mydec_sales.mydec_date
                    ELSE DATE_PARSE(SUBSTR(sales.saledt, 1, 10), '%Y-%m-%d')
                END
            ) - same_price_earlier_date) <= 365,
            FALSE
        ) AS sale_filter_same_sale_within_365,
        
        -- [Rest of your existing columns in unique_sales]
    FROM {{ source('iasworld', 'sales') }} AS sales
    LEFT JOIN calculated
        ON NULLIF(REPLACE(sales.instruno, 'D', ''), '')
        = calculated.instruno
    LEFT JOIN town_class AS tc
        ON sales.parid = tc.parid
        AND SUBSTR(sales.saledt, 1, 4) = tc.taxyr
    -- Join mydec_sales to bring in mydec_date
    LEFT JOIN mydec_sales
        ON NULLIF(REPLACE(sales.instruno, 'D', ''), '') = mydec_sales.doc_no
    WHERE sales.instruno IS NOT NULL
        AND sales.deactivat IS NULL
        AND sales.cur = 'Y'
        AND CAST(SUBSTR(sales.saledt, 1, 4) AS INT) BETWEEN 1997 AND YEAR(CURRENT_DATE)
        AND tc.township_code IS NOT NULL
        AND sales.price IS NOT NULL
)


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
        {{ source('sale', 'flag') }}
            AS sf
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
    unique_sales.sale_key,
    unique_sales.doc_no,
    unique_sales.deed_type,
    COALESCE(unique_sales.seller_name, mydec_sales.seller_name) AS seller_name,
    unique_sales.is_multisale,
    unique_sales.num_parcels_sale,
    COALESCE(unique_sales.buyer_name, mydec_sales.buyer_name) AS buyer_name,
    unique_sales.sale_type,
    unique_sales.sale_filter_same_sale_within_365,
    unique_sales.sale_filter_less_than_10k,
    unique_sales.sale_filter_deed_type,
    -- Our sales validation pipeline only validates sales past 2014 due to MyDec
    -- limitations. Previous to that values for sv_is_outlier will be NULL, so
    -- if we want to both exclude detected outliers and include sales prior to
    -- 2014, we need to code everything NULL as FALSE.
    COALESCE(sales_val.sv_is_outlier, FALSE) AS sale_filter_is_outlier,
    mydec_sales.mydec_deed_type,
    mydec_sales.sale_filter_ptax_flag,
    mydec_sales.mydec_property_advertised,
    mydec_sales.mydec_is_installment_contract_fulfilled,
    mydec_sales.mydec_is_sale_between_related_individuals_or_corporate_affiliates, -- noqa
    mydec_sales.mydec_is_transfer_of_less_than_100_percent_interest,
    mydec_sales.mydec_is_court_ordered_sale,
    mydec_sales.mydec_is_sale_in_lieu_of_foreclosure,
    mydec_sales.mydec_is_condemnation,
    mydec_sales.mydec_is_short_sale,
    mydec_sales.mydec_is_bank_reo_real_estate_owned,
    mydec_sales.mydec_is_auction_sale,
    mydec_sales.mydec_is_seller_buyer_a_relocation_company,
    mydec_sales.mydec_is_seller_buyer_a_financial_institution_or_government_agency, -- noqa
    mydec_sales.mydec_is_buyer_a_real_estate_investment_trust,
    mydec_sales.mydec_is_buyer_a_pension_fund,
    mydec_sales.mydec_is_buyer_an_adjacent_property_owner,
    mydec_sales.mydec_is_buyer_exercising_an_option_to_purchase,
    mydec_sales.mydec_is_simultaneous_trade_of_property,
    mydec_sales.mydec_is_sale_leaseback,
    mydec_sales.mydec_is_homestead_exemption,
    mydec_sales.mydec_homestead_exemption_general_alternative,
    mydec_sales.mydec_homestead_exemption_senior_citizens,
    mydec_sales.mydec_homestead_exemption_senior_citizens_assessment_freeze,
    sales_val.sv_is_outlier,
    sales_val.sv_is_ptax_outlier,
    sales_val.sv_is_heuristic_outlier,
    sales_val.sv_outlier_reason1,
    sales_val.sv_outlier_reason2,
    sales_val.sv_outlier_reason3,
    sales_val.sv_run_id,
    sales_val.sv_version
FROM unique_sales
LEFT JOIN mydec_sales
    ON unique_sales.doc_no = mydec_sales.doc_no
LEFT JOIN sales_val
    ON unique_sales.doc_no = sales_val.meta_sale_document_num;