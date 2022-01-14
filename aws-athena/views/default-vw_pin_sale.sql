-- View containing unique, filtered sales
CREATE OR replace VIEW default.vw_pin_sale
AS
  -- Township and class of associated PIN
  WITH townclass
       AS (SELECT DISTINCT parid,
                           class,
                           Substr(nbhd, 1, 2) AS township_code,
                           taxyr
           FROM   iasworld.pardat),
       -- Indicator to show which sales only have one observation by PIN and sale date
       singletons
       AS (SELECT parid,
                  saledt,
                  Count(*) AS obs
           FROM   iasworld.sales
           GROUP  BY parid,
                     saledt
           HAVING Count(*) = 1),
       -- Indicator for whether a given transaction number is the first associated with a sale
       drop_extra_transno
       AS (SELECT parid,
                  price,
                  transno,
                  Row_number()
                    over(
                      PARTITION BY parid, saledt, price
                      ORDER BY parid, saledt, price ) AS linenum
           FROM   iasworld.sales
           WHERE  transno IS NOT NULL),
       -- Only keep highest sale price when multiple exist for one pin on a given sale date
       highest_sp
       AS (SELECT parid,
                  saledt,
                  Max(price) AS max_price
           FROM   iasworld.sales
           GROUP  BY parid,
                     saledt),
       -- Remove transaction numbers that show up multiple times
       dedupe_transno
       AS (SELECT transno,
                  Count(transno) AS duplicate_transnos
           FROM   iasworld.sales
           WHERE  transno IS NOT NULL
           GROUP  BY transno
           HAVING Count(transno) = 1),
       unique_sales
       AS (SELECT DISTINCT sales.parid
                           AS
                           pin,
                           Substr(sales.saledt, 1, 4)
                           AS
                           year,
                           townclass.township_code,
                           townclass.class,
                           Date_parse(Substr(sales.saledt, 1, 10), '%Y-%m-%d')
                           AS
                              sale_date,
                           Cast(sales.price AS BIGINT)
                           AS
                              sale_price,
                           Log(sales.price, 10)
                           AS
                              sale_price_log10,
                           sales.salekey
                           AS
                           sale_key
                              ,
                           Nullif(sales.transno, '')
                              AS doc_no,
                           Nullif(sales.instrtyp, '')
                           AS
                              deed_type,
                           Nullif(sales.oldown, '')
                           AS
                              seller_name,
                           Nullif(sales.own1, '')
                           AS
                              buyer_name,
                           CASE
                             WHEN sales.saletype = '0' THEN 'LAND'
                             WHEN sales.saletype = '1' THEN 'LAND AND BUILDING'
                           END
                           AS
                              sale_type
           FROM   iasworld.sales
                  left join townclass
                         ON sales.parid = townclass.parid
                            AND Substr(sales.saledt, 1, 4) = townclass.taxyr
                  left join singletons
                         ON sales.parid = singletons.parid
                            AND sales.saledt = singletons.saledt
                  left join drop_extra_transno
                         ON sales.parid = drop_extra_transno.parid
                            AND sales.price = drop_extra_transno.price
                            AND sales.transno = drop_extra_transno.transno
                  inner join highest_sp
                          ON sales.parid = highest_sp.parid
                             AND sales.saledt = highest_sp.saledt
                             AND sales.price = highest_sp.max_price
                  inner join dedupe_transno
                          ON sales.transno = dedupe_transno.transno
           WHERE  ( ( sales.transno IS NOT NULL
                      AND linenum = 1 )
                     OR obs = 1 )
                  -- nopar is number of parcels sold
                  AND nopar = 1
                  AND sales.price > 10000
                  AND sales.parid != '0'
                  AND sales.transno != '1'
                  AND Cast(Substr(sales.saledt, 1, 4) AS INT) BETWEEN
                      1997 AND Year(current_date)
                  -- Exclude quit claims, executor deeds, beneficial interests
                  AND instrtyp NOT IN ( '03', '04', '06' )),
       -- Join on lower and upper bounds so that outlier sales can be filtered out
       sale_filter
       AS (SELECT township_code,
                  class,
                  year,
                  Avg(sale_price_log10) - STDDEV(sale_price_log10) * 4 AS
                     sale_filter_lower_limit,
                  Avg(sale_price_log10) + STDDEV(sale_price_log10) * 4 AS
                     sale_filter_upper_limit,
                  Count(*)
                  sale_filter_count
           FROM   unique_sales
           GROUP  BY township_code,
                     class,
                     year)
  SELECT unique_sales.*,
         sale_filter_lower_limit,
         sale_filter_upper_limit,
         sale_filter_count
  FROM   unique_sales
         left join sale_filter
                ON unique_sales.township_code = sale_filter.township_code
                   AND unique_sales.class = sale_filter.class
                   AND unique_sales.year = sale_filter.year
