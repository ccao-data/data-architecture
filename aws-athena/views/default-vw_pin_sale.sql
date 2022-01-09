-- View containing unique, filtered sales
CREATE OR REPLACE VIEW default.vw_pin_sale AS
-- Township and class of associated PIN
WITH townclass AS (
    SELECT DISTINCT
        parid,
        class,
        SUBSTR(nbhd, 1, 2) AS township_code,
        taxyr
    FROM iasworld.pardat
),
-- Indicator to show which sales only have one observation by PIN and sale date
singletons AS (
    SELECT
        parid,
        saledt,
        COUNT(*) AS obs
    FROM iasworld.sales
    GROUP BY parid, saledt HAVING COUNT(*) = 1
),
-- Indicator for whether a given transaction number is the first associated with a sale
drop_extra_transno AS (
    SELECT
        parid,
        price,
        transno,
        ROW_NUMBER() OVER(
            PARTITION BY parid, saledt, price
            ORDER BY parid, saledt, price
        ) AS linenum
    FROM iasworld.sales
    WHERE transno IS NOT NULL
),
-- Only keep highest sale price when multiple exist for one pin on a given sale date
highest_sp AS (
    SELECT
        parid,
        saledt,
        MAX(price) AS max_price
    FROM iasworld.sales
    GROUP BY parid, saledt
),
-- Remove transaction numbers that show up multiple times
dedupe_transno AS (
    SELECT
        transno,
        COUNT(transno) AS duplicate_transnos
    FROM iasworld.sales
    WHERE transno IS NOT NULL
    AND nopar = 1
    GROUP BY transno
),
unique_sales AS (
    SELECT DISTINCT
        sales.parid AS pin,
        SUBSTR(sales.saledt, 1, 4) AS year,
        townclass.township_code,
        townclass.class,
        date_parse(SUBSTR(sales.saledt, 1, 10), '%Y-%m-%d') AS sale_date,
        CAST(sales.price AS bigint) AS sale_price,
        LOG(sales.price, 10) AS sale_price_log10,
        sales.salekey AS sale_key,
        NULLIF(sales.transno, '') AS doc_no,
        NULLIF(sales.instrtyp, '') AS deed_type,
        NULLIF(sales.oldown, '') AS seller_name,
        NULLIF(sales.own1, '') AS buyer_name,
        CASE
        	WHEN sales.saletype = '0' THEN 'LAND'
        	WHEN sales.saletype = '1' THEN 'LAND AND BUILDING'
        END AS sale_type
    FROM iasworld.sales
    LEFT JOIN townclass
        ON sales.parid = townclass.parid
        AND SUBSTR(sales.saledt, 1, 4) = townclass.taxyr
    LEFT JOIN singletons
        ON sales.parid = singletons.parid
        AND sales.saledt = singletons.saledt
    LEFT JOIN drop_extra_transno
        ON sales.parid = drop_extra_transno.parid
        AND sales.price = drop_extra_transno.price
        AND sales.transno = drop_extra_transno.transno
    INNER JOIN highest_sp
        ON sales.parid = highest_sp.parid
        AND sales.saledt = highest_sp.saledt
        AND sales.price = highest_sp.max_price
    LEFT JOIN dedupe_transno
        ON sales.transno = dedupe_transno.transno
    -- nopar is number of parcels sold
    WHERE ((sales.transno IS NOT NULL
            AND nopar = 1
            AND duplicate_transnos = 1
            AND linenum = 1)
        OR obs = 1)
    AND sales.price > 10000
    AND CAST(SUBSTR(sales.saledt, 1, 4) AS int)
        BETWEEN 1997 AND YEAR(current_date)
    -- Exclude quit claims, executor deeds, beneficial interests
    AND instrtyp NOT IN ('03', '04', '06')
),
-- Join on lower and upper bounds so that outlier sales can be filtered out
sale_filter AS (
    SELECT
        township_code,
        class,
        year,
        AVG(sale_price_log10) - STDDEV(sale_price_log10) * 4 AS sale_filter_lower_limit,
        AVG(sale_price_log10) + STDDEV(sale_price_log10) * 4 AS sale_filter_upper_limit,
        COUNT(*) sale_filter_count
    FROM unique_sales
    GROUP BY township_code, class, year
)
SELECT
    unique_sales.*,
    sale_filter_lower_limit,
    sale_filter_upper_limit,
    sale_filter_count
FROM unique_sales
LEFT JOIN sale_filter
    ON unique_sales.township_code = sale_filter.township_code
    AND unique_sales.class = sale_filter.class
    AND unique_sales.year = sale_filter.year