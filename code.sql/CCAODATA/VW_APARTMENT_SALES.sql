ALTER VIEW VW_APARTMENT_SALES AS

/*
The goal of this view is to return a "clean" set of sales with information such
as date, price, deed type, and, buyer and seller names. Some problems this view is
correcting for:
  - Duplicate or multiple sales on the same day for a given PIN
  - Duplicate PINs by doc_no
  - Incorrect sale dates
*/

--- Gather information on the seller and buyer from the Cook County Recorder of Deeds
WITH CCRD AS (
	SELECT
		PIN,
		CASE WHEN YEAR(RECORDED_DATE) < 2003 THEN CONCAT('000', DOC_NO)
			 WHEN YEAR(RECORDED_DATE) BETWEEN 2003 AND 2009 THEN CONCAT('00', DOC_NO)
		     WHEN YEAR(RECORDED_DATE) > 2009 THEN CONCAT('0', DOC_NO)
			 END AS DOC_NO,
		EXECUTED_DATE,
		SELLER_NAME,
		BUYER_NAME
	FROM DTBL_CCRDSALES
)

SELECT DISTINCT
	SALES.PIN,
	CLASS,
	township_name AS township,
	CONVERT(DATE, SALES.RECORDED_DATE) as sale_date,
	YEAR(SALES.RECORDED_DATE) AS sale_year,
	CAST(SALES.SALE_PRICE AS INT) as sale_price,
	VALID
FROM IDORSALES AS SALES

/* Goal here is to remove duplicates by finding the maximum sale price per PIN per day
(such that the highest sale for each day is the one used), then by taxing the maximum
DOC_NO per PIN per day for all sales with the maximum sale_price */
INNER JOIN (
	SELECT a.PIN, a.RECORDED_DATE, MAX(a.DOC_NO) AS DOC_NO
	FROM IDORSALES a
	INNER JOIN (
		SELECT PIN, RECORDED_DATE, MAX(SALE_PRICE) AS MAX_PRICE
		FROM IDORSALES
		GROUP BY PIN, RECORDED_DATE
	) AS b
	ON b.PIN = a.PIN AND b.RECORDED_DATE = a.RECORDED_DATE AND b.MAX_PRICE = a.SALE_PRICE
	GROUP BY a.PIN, a.RECORDED_DATE, b.MAX_PRICE
) AS MAX_SALE
ON MAX_SALE.PIN = SALES.PIN
AND MAX_SALE.RECORDED_DATE = SALES.RECORDED_DATE
AND MAX_SALE.DOC_NO = SALES.DOC_NO

--- Some sales show up more than once with dates transcribed incorrectly as Jan rather than Oct, Nov, or Dec */
INNER JOIN (
	SELECT
		DOC_NO,
		MAX(RECORDED_DATE) AS CORRECT_DATE
	FROM IDORSALES
	GROUP BY DOC_NO
) PROBLEM_DATES
ON SALES.DOC_NO = PROBLEM_DATES.DOC_NO AND SALES.RECORDED_DATE = PROBLEM_DATES.CORRECT_DATE

--- Add names from CCRD sales data
LEFT JOIN CCRD
ON SALES.PIN = CCRD.PIN
AND SALES.DOC_NO = CCRD.DOC_NO

--- Add validated indicator
LEFT JOIN (SELECT DEED_NUMBER, VALID FROM DTBL_VALIDATED_IC_SALES) VALID
ON SALES.DOC_NO = VALID.DEED_NUMBER

-- Join HEAD for township and class
LEFT JOIN (SELECT PIN, TAX_YEAR, HD_CLASS AS CLASS, LEFT(HD_TOWN, 2) AS TOWN FROM AS_HEADT) HEAD
ON SALES.PIN = HEAD.PIN AND YEAR(SALES.RECORDED_DATE) = HEAD.TAX_YEAR

--- Join
LEFT JOIN FTBL_TOWNCODES TOWNS
ON HEAD.TOWN = TOWNS.township_code

WHERE SALES.DEED_TYPE NOT IN ('Q', 'E', 'B')
AND MULT_IND = ''
AND YEAR(SALES.RECORDED_DATE) >= 1997
AND (SALES.DEED_TYPE != '' OR YEAR(SALES.RECORDED_DATE) <= 2000)
AND VALID IS NOT NULL
AND CLASS IN (313, 314, 315, 318, 391, 396)
AND triad_name = 'City'