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
       -- "nopar" isn't entirely accurate for sales associated with only one parcel, so we create our own counter
       calculated
       AS (SELECT instruno,
                  Count(*) AS nopar_calculated
           FROM   iasworld.sales
           WHERE  deactivat IS NULL
           GROUP  BY instruno),
       unique_sales
       AS (SELECT *
           FROM   (SELECT DISTINCT sales.parid
                                   AS
                                           pin,
                                   Substr(sales.saledt, 1, 4)
                                   AS
                                           year,
                                   townclass.township_code,
                                   townclass.class,
                                   Date_parse(Substr(sales.saledt, 1, 10),
                                   '%Y-%m-%d')
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
                                           sale_key,
                                   Nullif(sales.instruno, '')
                                   AS
                                           doc_no,
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
                                     WHEN sales.saletype = '1' THEN
                                     'LAND AND BUILDING'
                                   END
                                   AS
                                           sale_type,
                                   -- Sales are not entirely unique by pin/date so we group all sales b pin/date
                                   -- then order then order by descending price and give the top observation a value of 1 for "linenum"
                                   Row_number()
                                     over(
                                       PARTITION BY sales.parid, sales.saledt
                                       ORDER BY sales.parid, sales.saledt, -1 *
                                     sales.price ) AS
                                           linenum
                   FROM   iasworld.sales
                          left join calculated
                                 ON sales.instruno = calculated.instruno
                          left join townclass
                                 ON sales.parid = townclass.parid
                                    AND Substr(sales.saledt, 1, 4) =
                                        townclass.taxyr
                   WHERE  sales.instruno IS NOT NULL
                          -- Indicates whether a record has been deactivated
                          AND sales.deactivat IS NULL
                          AND calculated.nopar_calculated = 1
                          -- "nopar" is number of parcels sold
                          AND sales.nopar <= 1
                          AND sales.price > 10000
                          AND Cast(Substr(sales.saledt, 1, 4) AS INT) BETWEEN
                              1997 AND Year(current_date)
                          -- Exclude quit claims, executor deeds, beneficial interests
                          AND instrtyp NOT IN ( '03', '04', '06' ))
           -- Only use max price by pin/sale date
           WHERE  linenum = 1),
       -- Lower and upper bounds so that outlier sales can be filtered out
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