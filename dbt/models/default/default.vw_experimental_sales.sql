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
            year_of_sale
        FROM {{ source('sale', 'mydec') }}
        WHERE line_2_total_parcels = 1 -- Remove multisales
    )
    /* Some sales in mydec have multiple rows for one pin on a given sale date.
    Sometimes they have different dates than iasworld prior to 2021 and when
    joined back onto unique_sales will create duplicates by pin/sale date. */
    WHERE num_single_day_sales = 1
        OR (YEAR(mydec_date) > 2020)
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
    unique_sales.sale_filter_deed_type
    -- Our sales validation pipeline only validates sales past 2014 due to MyDec
    -- limitations. Previous to that values for sv_is_outlier will be NULL, so
    -- if we want to both exclude detected outliers and include sales prior to
    -- 2014, we need to code everything NULL as FALSE.
FROM unique_sales
LEFT JOIN mydec_sales
    ON unique_sales.doc_no = mydec_sales.doc_no;
