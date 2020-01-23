/* This view provides pre-selected residential sales ratios for the purpose of reporting ratio study statistics.
This view and its code is goverened by the Standard Operating Procedures on Residential Sales Ratio Studies.

!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
DO NOT ALTER THIS VIEW WITHOUT AUTHORIZATION FROM A SENIOR DATA SCIENTISTS OR THE CHIEF DATA OFFICER
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
*/

ALTER VIEW VW_RES_RATIOS AS

SELECT
 T.PIN
/* --------------- SOPs on Ratio Studies, 3.3.2.3 --------------- */
 , CASE WHEN T.TAX_YEAR>=YEAR(GETDATE())-1 THEN
 (fitted_value_6)/PRIO_YEAR_SALE.sale_price 
	WHEN T.TAX_YEAR<YEAR(GETDATE())-1 THEN (fitted_value_6)/NEXT_YEAR_SALE.sale_price 
	 END AS [RATIO ON PIPELINE RESULT]
, version AS [PIPELINE VERSION]
, CASE WHEN T.TAX_YEAR>=YEAR(GETDATE())-1 THEN ((T.HD_ASS_LND + T.HD_ASS_BLD)*10)/PRIO_YEAR_SALE.sale_price 
WHEN T.TAX_YEAR<YEAR(GETDATE())-1 THEN ((T.HD_ASS_LND + T.HD_ASS_BLD)*10)/NEXT_YEAR_SALE.sale_price 
	END AS [RATIO ON FIRST PASS]
, CASE WHEN T.TAX_YEAR>=YEAR(GETDATE())-1 THEN ((TB.HD_ASS_LND + TB.HD_ASS_BLD)* 10)/PRIO_YEAR_SALE.sale_price
 WHEN T.TAX_YEAR<YEAR(GETDATE())-1 THEN ((TB.HD_ASS_LND + TB.HD_ASS_BLD)*10)/NEXT_YEAR_SALE.sale_price 
	END AS [RATIO ON ASSESSOR CERTIFIED]
, CASE WHEN T.TAX_YEAR>=YEAR(GETDATE())-1 THEN ((BR.HD_ASS_LND + BR.HD_ASS_BLD)* 10)/PRIO_YEAR_SALE.sale_price 
WHEN T.TAX_YEAR<YEAR(GETDATE())-1 THEN ((BR.HD_ASS_LND + BR.HD_ASS_BLD)* 10)/NEXT_YEAR_SALE.sale_price 
	END AS [RATIO ON BOARD CERTIFIED] 
/* -------------------------------------------------------------- */
, CASE WHEN T.TAX_YEAR>=YEAR(GETDATE())-1 THEN PRIO_YEAR_SALE.sale_price 
	   WHEN T.TAX_YEAR<YEAR(GETDATE())-1 THEN NEXT_YEAR_SALE.sale_price 
	END AS [SALE PRICE] 
, CASE WHEN T.TAX_YEAR>=YEAR(GETDATE())-1 THEN PRIO_YEAR_SALE.DOC_NO 
	   WHEN T.TAX_YEAR<YEAR(GETDATE())-1 THEN NEXT_YEAR_SALE.DOC_NO
	END AS [DEED NUMBER]
/* --------------- SOPs on Ratio Studies, 3.1 ------------------- */
, CASE WHEN T.HD_CLASS IN (202, 203, 204, 205, 206, 207, 208, 209, 210, 234, 278, 295) THEN 'Single_Family'
  WHEN T.HD_CLASS IN (211, 212) THEN 'Multi-Family'
  WHEN T.HD_CLASS IN (200, 201, 241) THEN 'Vacant Land'
  WHEN T.HD_CLASS = 299 AND ((T.HD_PRI_BLD + T.HD_PRI_LND) > 10 OR (T.HD_PRI_BLD + T.HD_PRI_LND) IS NULL) THEN 'Residential Condominium' 
  ELSE 'Error' END AS [modeling_group] 
/* -------------------------------------------------------------- */
, township_name, CAST(T.HD_NBHD AS varchar) AS NBHD, T.TAX_YEAR AS YEAR 
FROM AS_HEADT AS T
LEFT JOIN
AS_HEADTB AS TB
ON T.PIN=TB.PIN AND T.TAX_YEAR=TB.TAX_YEAR
LEFT JOIN
AS_HEADBR AS BR
ON T.PIN=BR.PIN AND T.TAX_YEAR=BR.TAX_YEAR
LEFT JOIN 
VW_CLEAN_IDORSALES AS PRIO_YEAR_SALE
ON T.PIN=PRIO_YEAR_SALE.PIN AND T.TAX_YEAR=YEAR(PRIO_YEAR_SALE.sale_date)+1
LEFT JOIN 
VW_CLEAN_IDORSALES AS NEXT_YEAR_SALE
ON T.PIN=NEXT_YEAR_SALE.PIN AND T.TAX_YEAR=YEAR(NEXT_YEAR_SALE.sale_date)-1
LEFT JOIN
DTBL_MODELVALS AS M
ON T.PIN=M.PIN AND T.TAX_YEAR=M.TAX_YEAR
LEFT JOIN
FTBL_TOWNCODES AS TC
ON TC.township_code=LEFT(T.HD_TOWN, 2)
WHERE (1=1)
	/* SOP 3.5.1 */
	AND (PRIO_YEAR_SALE.sale_price > 10000 AND PRIO_YEAR_SALE.PIN IS NOT NULL 
		OR
		NEXT_YEAR_SALE.sale_price >10000 AND NEXT_YEAR_SALE.PIN IS NOT NULL)
	/* SOPs 3.1.1 */
	AND CONCAT(LEFT(T.HD_TOWN,2), T.HD_NBHD) != '23171'
	AND T.HD_CLASS IN (202, 203, 204, 205, 206, 207, 208, 209, 210, 234, 278, 295, 211, 212, 200, 201, 241, 299)