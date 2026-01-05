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
        -- Indicates whether a record has been deactivated
            AND sales.deactivat IS NULL
            AND sales.cur = 'Y'
            AND CAST(SUBSTR(sales.saledt, 1, 4) AS INT) BETWEEN 1997 AND YEAR(
                CURRENT_DATE
            )
            AND tc.township_code IS NOT NULL
            AND sales.price IS NOT NULL
    )
    -- Only use max price by pin/sale date
    WHERE max_price = 1
        AND (bad_doc_no = 1 OR is_multisale = TRUE)
),

mydec_sales AS (
    SELECT * FROM (
        SELECT
            REPLACE(document_number, 'D', '') AS doc_no,
            REPLACE(line_1_primary_pin, '-', '') AS pin,
            DATE_PARSE(line_4_instrument_date, '%Y-%m-%d') AS mydec_date,
            line_5_instrument_type AS mydec_deed_type,
            NULLIF(TRIM(seller_name), '') AS seller_name,
            NULLIF(TRIM(buyer_name), '') AS buyer_name,
            line_7_property_advertised AS mydec_property_advertised,
            line_9_no_changes AS mydec_line_9_no_changes,
            line_9_demolitiondamage AS mydec_line_9_demolition_damage,
            line_9_additions AS mydec_line_9_additions,
            line_9_major_remodeling AS mydec_line_9_major_remodeling,
            line_9_new_construction AS mydec_line_9_new_construction,
            line_9_other_change AS mydec_line_9_other_change,
            line_9_other_change_description
                AS mydec_line_9_other_change_description,
            line_9_date_of_significant_change
                AS mydec_line_9_date_of_significant_change,
            line_10a
                AS mydec_is_installment_contract_fulfilled,
            line_10b
                AS mydec_is_sale_between_related_individuals_or_corporate_affiliates, -- noqa
            line_10c
                AS mydec_is_transfer_of_less_than_100_percent_interest,
            line_10d AS mydec_is_court_ordered_sale,
            line_10e AS mydec_is_sale_in_lieu_of_foreclosure,
            line_10f AS mydec_is_condemnation,
            line_10g AS mydec_is_short_sale,
            line_10h AS mydec_is_bank_reo_real_estate_owned,
            line_10i AS mydec_is_auction_sale,
            line_10j AS mydec_is_seller_buyer_a_relocation_company,
            line_10k
                AS mydec_is_seller_buyer_a_financial_institution_or_government_agency, -- noqa
            line_10l AS mydec_is_buyer_a_real_estate_investment_trust,
            line_10m AS mydec_is_buyer_a_pension_fund,
            line_10n AS mydec_is_buyer_an_adjacent_property_owner,
            line_10o AS mydec_is_buyer_exercising_an_option_to_purchase,
            line_10p AS mydec_is_simultaneous_trade_of_property,
            line_10q AS mydec_is_sale_leaseback,
            line_10s AS mydec_is_homestead_exemption,
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
                line_10b OR line_10c OR line_10d OR line_10e OR line_10f
                OR line_10g OR line_10h OR line_10i OR line_10k
            ) AS sale_filter_ptax_flag,
            COUNT() OVER (
                PARTITION BY line_1_primary_pin, line_4_instrument_date
            ) AS num_single_day_sales,
            year_of_sale
        FROM {{ source('sale', 'mydec') }}
        WHERE NOT is_multisale
    )
    /* Some sales in mydec have multiple rows for one pin on a given sale date.
    Sometimes they have different dates than iasworld prior to 2021 and when
    joined back onto unique_sales will create duplicates by pin/sale date. */
    WHERE num_single_day_sales = 1
        OR (year_of_sale > '2020')
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
    mydec_sales.mydec_line_9_no_changes,
    mydec_sales.mydec_line_9_demolition_damage,
    mydec_sales.mydec_line_9_additions,
    mydec_sales.mydec_line_9_major_remodeling,
    mydec_sales.mydec_line_9_new_construction,
    mydec_sales.mydec_line_9_other_change,
    mydec_sales.mydec_line_9_other_change_description,
    mydec_sales.mydec_line_9_date_of_significant_change,
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
    sales_val.sv_version,
    flag_override.is_arms_length,
    flag_override.is_flip,
    flag_override.has_class_change,
    flag_override.has_characteristic_change,
    flag_override.requires_field_check,
    CASE
        -- If there is an override, use override logic
        WHEN
            flag_override.is_arms_length IS NOT NULL
            OR flag_override.is_flip IS NOT NULL
            OR flag_override.has_class_change IS NOT NULL
            OR flag_override.has_characteristic_change IS NOT NULL
            OR flag_override.requires_field_check IS NOT NULL
        THEN NOT (
                flag_override.is_arms_length = FALSE
                OR flag_override.is_flip = TRUE
                OR flag_override.has_class_change = TRUE
                OR flag_override.has_characteristic_change = 'yes_major'
                OR flag_override.requires_field_check = TRUE
        )
        -- If there is no override, default to sv_is_outlier
        WHEN sales_val.sv_is_outlier IS NOT NULL
            THEN NOT sales_val.sv_is_outlier
        -- If neither override nor sv_is_outlier is populated, leave null
        ELSE NULL
    END AS is_valid_for_modeling
FROM unique_sales
LEFT JOIN mydec_sales
    ON unique_sales.doc_no = mydec_sales.doc_no
LEFT JOIN sales_val
    ON unique_sales.doc_no = sales_val.meta_sale_document_num
LEFT JOIN {{ source('sale', 'flag_override') }} AS flag_override
    ON unique_sales.doc_no = flag_override.doc_no
