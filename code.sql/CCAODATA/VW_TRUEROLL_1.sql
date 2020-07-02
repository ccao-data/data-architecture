/* This view produces an 'exemption roll' for use in the TrueRoll application */

ALTER VIEW VW_TRUEROLL_1 AS

SELECT ROLL.*
, exemption_years 
, SALES.most_recent_sale_date as [deed_date] /* Note: This is recorded date, Note: not validated well */
, CCRDSALES.BUYER_NAME AS [BUYER NAME ON DEED]
, SALES.DOC_NO AS [DEED NUMBER]
FROM ( SELECT
	T.PIN as [parcel_num], [KEY PIN] /* properties where the the home's footprint spans multiple PINs are unique by key pin */
	, T.HD_CLASS AS [PROPERTY CLASS]
	, T.TAX_YEAR
	, MPIN.IS_ACROSS_MULT_PINS
	, PARKING.IS_PARKING_SPACE
	/* Addresses */
	, GEO.PROPERTY_ADDRESS
	, GEO.PROPERTY_APT_NO
	, GEO.PROPERTY_CITY [situs_city]
	, 'IL' AS [situs_state]
	, LEFT(GEO.PROPERTY_ZIP, 5) AS [situs_zip5]
	, RIGHT(GEO.PROPERTY_ZIP, 4) AS [situs_zip_plus4]
	, GEO.MAILING_ADDRESS
	, GEO.MAILING_CITY as [mail_city]
	, GEO.MAILING_STATE as [mail_state]
	, LEFT(GEO.MAILING_ZIP, 5) AS [mail_zip5]
	, RIGHT(GEO.MAILING_ZIP, 4) AS [mail_zip_plus4]
	
	/* Other location data */
	, GEO.centroid_x as [situs_latitude]
	, GEO.centroid_y as [situs_longitude]

	, T.HD_NAME as [MAILING_NAME] /* We don't have 'owners' names. We have mailing names. */
	, SREX.NAME as [SENIOR EXEMPTION NAME] /* We have full names for seniors receiving either the senior exemption or the senior freeze */
	, SREX.BIRTH_DATE AS [SENIOR EXEMPTION BIRTHDATE]
	, SREX.YR_APPL AS [YEAR SENIOR APPLIED]

	/* Values */
	, (BR.HD_ASS_BLD+BR.HD_ASS_LND)*10 AS [market_value]
	, ROUND((BR.HD_ASS_BLD+BR.HD_ASS_LND)*FACTOR, 0) AS [PRE-EXEMPTION TAXABLE VALUE] /* This is NOT valid for DVET_GE75 */
	, CASE WHEN exemption_codes IS NULL THEN '0, 0, 0, 0, 0, 0, 0, 0' ELSE exemption_codes END AS exemption_codes
	, CASE WHEN exemption_value IS NULL THEN 0 ELSE exemption_value END AS exemption_value 

	FROM AS_HEADT AS T /* HEADT defines the universe of PINs for that year. It should be identical in terms of PINs to HEADBR, which is the Board of Review's file */
	INNER JOIN 
	AS_HEADBR AS BR
	ON BR.PIN=T.PIN AND BR.TAX_YEAR=T.TAX_YEAR
	INNER JOIN 
	FTBL_EQUALIZATION_FACTOR AS FACTOR /* Equalization factor is necessary to calculate pre-tax values */
	ON FACTOR.TAX_YEAR=T.TAX_YEAR

	LEFT JOIN /* Want to include Key Pins, which are stored in the detail table in the AS-400 */
		(SELECT PIN, CASE WHEN DT_KEY_PIN=0 THEN NULL ELSE DT_KEY_PIN END AS [KEY PIN] , TAX_YEAR
			FROM AS_DETAILT 
			WHERE LEFT(DT_CLASS,1)=2 AND DT_CLASS NOT IN (200, 288) AND TAX_YEAR=2018 AND DT_MLT_CD=1
			) AS KEYPINS
	ON KEYPINS.PIN=T.PIN
	
	/*
	Simple query to find PINs whose structure is not entirely within the PIN bounds. Any PIN
	single-family residential PIN with a proration rate not equal to 100% (DT_PER_ASS, but 100% is actually 0)
	means that the structure on that PIN is actually spread across multiple PINs
	*/
	LEFT JOIN (
		SELECT DISTINCT DT.PIN, 1 AS IS_ACROSS_MULT_PINS
		FROM AS_DETAILT DT
		WHERE DT_CLASS NOT IN (200, 288, 299) AND DT_PER_ASS != 0
	) MPIN ON MPIN.PIN = T.PIN

	/*
	Detect parking spaces by looking for the GR CDU code
	*/
	LEFT JOIN (
		SELECT DISTINCT DT.PIN, 1 AS IS_PARKING_SPACE
		FROM AS_DETAILT DT
		WHERE DT_CLASS = 299 AND DT_CDU = 'GR'
	) PARKING ON PARKING.PIN = T.PIN

	LEFT JOIN 
	(SELECT DISTINCT /* I copied the entire VW_PINGEO into this subsection so that this view did not leverage another view */

CASE WHEN PL_PIN IS NULL THEN lox.PIN ELSE PL_PIN END AS PIN
/* PROPLOCS VARIABLES */
, REPLACE(LTRIM(RTRIM(CAST(PL_HOUSE_NO AS varchar(10)))) + ' '
+ LTRIM(RTRIM(PL_DIR)) + ' '
+ LTRIM(RTRIM(PL_STR_NAME)) + ' '
+ LTRIM(RTRIM(PL_STR_SUFFIX)), '  ', ' ') AS PROPERTY_ADDRESS
, PL_APT_NO AS PROPERTY_APT_NO
, LTRIM(RTRIM(PL_CITY_NAME)) AS PROPERTY_CITY
, LEFT(PL_ZIPCODE, 5) + '-' + RIGHT(PL_ZIPCODE, 4) AS PROPERTY_ZIP
/* HEAD VARIABLES */
, RIGHT(LTRIM(RTRIM(HD_CITY)), 2) AS MAILING_STATE, HD_ADDR AS MAILING_ADDRESS,
CASE 
	WHEN LEN(LTRIM(RTRIM(HD_CITY))) >= 2 THEN LTRIM(RTRIM( LEFT(LTRIM(RTRIM(HD_CITY)), LEN(LTRIM(RTRIM(HD_CITY))) - 2) ))
	WHEN LEN(LTRIM(RTRIM(HD_CITY))) < 2 THEN HD_CITY
END AS MAILING_CITY
, HD_ZIP AS MAILING_ZIP
/* PROPLOCS VARIABLES */
, PL_TOWN AS township
/* HEAD VARIABLES */
, HD_NBHD AS nbhd
/* TOWNCODE VARIABLES */
, township_name
/* PINLOCATIONS VARIABLES */
, GEOID AS geoid, TRACTCE AS census_tract, CASE WHEN ward = '' THEN NULL ELSE ward END AS ward, ohare_noise, floodplain
, withinmr100, withinmr101300
, commissioner_dist, reps_dist, senate_dist
, tif_agencynum, PUMA, municipality, FIPS, midincome
, white_perc, black_perc, his_perc, other_perc
, CONVERT(FLOAT, centroid_x) AS centroid_x, CONVERT(FLOAT, centroid_y) AS centroid_y
/* HEAD VARIABLES */
, TAX_YEAR AS most_recent
/* JOIN KEY */
, CASE WHEN LEN(CAST(HD_NBHD AS varchar)) = 2 THEN CAST(PL_TOWN AS varchar) + '0' + CAST(HD_NBHD AS varchar)
ELSE CAST(PL_TOWN AS varchar) + CAST(HD_NBHD AS varchar) END AS [town_nbhd.KEY]
/* Indicators for which data is not missing */
, CASE WHEN PL_PIN IS NOT NULL THEN 1 ELSE NULL END AS PROPLOCS, PINLOCATIONS

FROM PROPLOCS AS prop

INNER JOIN
	(SELECT C.* 
FROM (SELECT DISTINCT HD_PIN from AS_HEADT) A
CROSS APPLY (SELECT TOP 1 * 
             FROM AS_HEADT B
             WHERE A.HD_PIN = B.HD_PIN 
             ORDER by TAX_YEAR DESC) C) h
	ON h.PIN = prop.PL_PIN
FULL JOIN
	(SELECT *, 1 AS PINLOCATIONS FROM DTBL_PINLOCATIONS WHERE primary_polygon = 1 AND PIN999 != 1) lox
	ON LEFT(prop.PL_PIN, 10) = LEFT(lox.PIN, 10)
LEFT JOIN
	(SELECT township_code, township_name FROM FTBL_TOWNCODES) town
	on prop.PL_TOWN = town.township_code) AS GEO
	ON GEO.PIN=T.PIN
	LEFT JOIN
	SENIOREXEMPTIONS AS SREX /* Special table of senior's receiving the senior exemptions (homestead, freeze) */
	ON SREX.PIN=T.PIN AND SREX.TAX_YEAR=T.TAX_YEAR 
	LEFT JOIN
	/* Generates the exemption_codes field */
		(SELECT PIN, TAX_YEAR,
	CONCAT(f_HO, ', ', f_HS, ', ',f_SF , ', ',f_DISABLED , ', ',f_VETERAN , ', ',f_RETURN_VET , ', ',
	 f_DVET_LT75 , ', ', f_DVET_GE75) AS exemption_codes
	 , (HO + HS + SF + DISABLED + VETERAN + RETURN_VET + DVET_LT75 + DVET_GE75) AS exemption_value
	 FROM (
	SELECT *
		, CASE WHEN HO>0 THEN 1 ELSE 0 END AS f_HO
		, CASE WHEN HS>0 THEN 1 ELSE 0 END AS f_HS
		, CASE WHEN SF>0 THEN 1 ELSE 0 END AS f_SF
		, CASE WHEN DISABLED>0 THEN 1 ELSE 0 END AS f_DISABLED
		, CASE WHEN VETERAN>0 THEN 1 ELSE 0 END AS f_VETERAN
		, CASE WHEN RETURN_VET>0 THEN 1 ELSE 0 END AS f_RETURN_VET
		, CASE WHEN DVET_LT75>0 THEN 1 ELSE 0 END AS f_DVET_LT75
		, CASE WHEN DVET_GE75>0 THEN 1 ELSE 0 END AS f_DVET_GE75
		FROM EXEMPTIONS
		) AS X) AS EX
	ON T.PIN=EX.PIN AND T.TAX_YEAR=EX.TAX_YEAR
	WHERE (1=1)
	AND T.TAX_YEAR=(SELECT MAX(TAX_YEAR) FROM EXEMPTIONS)
	AND T.HD_CLASS IN (202, 203, 204, 205, 206, 207, 208, 209, 210, 234, 278, 295, 299, 211, 212, 213)
) AS ROLL

/* Emeption years */
INNER JOIN (
SELECT PIN
, CONCAT(IND1, ', ', IND2, ', ', IND3, ', ', IND4, ', ', IND5, ', ', IND6, ', ', IND7, ', ', IND8, ', ', IND9, ', ', IND10) AS exemption_years
FROM(
SELECT T.PIN
, CASE WHEN E1.PIN IS NULL THEN CONCAT('[', Year(GetDate())-1, ':FALSE]') ELSE CONCAT('[', Year(GetDate())-1, ':TRUE]') END AS IND1
, CASE WHEN E2.PIN IS NULL THEN CONCAT('[', Year(GetDate())-2, ':FALSE]') ELSE CONCAT('[', Year(GetDate())-2, ':TRUE]') END AS IND2
, CASE WHEN E3.PIN IS NULL THEN CONCAT('[', Year(GetDate())-3, ':FALSE]') ELSE CONCAT('[', Year(GetDate())-3, ':TRUE]') END AS IND3
, CASE WHEN E4.PIN IS NULL THEN CONCAT('[', Year(GetDate())-4, ':FALSE]') ELSE CONCAT('[', Year(GetDate())-4, ':TRUE]') END AS IND4
, CASE WHEN E5.PIN IS NULL THEN CONCAT('[', Year(GetDate())-5, ':FALSE]') ELSE CONCAT('[', Year(GetDate())-5, ':TRUE]') END AS IND5
, CASE WHEN E6.PIN IS NULL THEN CONCAT('[', Year(GetDate())-6, ':FALSE]') ELSE CONCAT('[', Year(GetDate())-6, ':TRUE]') END AS IND6
, CASE WHEN E7.PIN IS NULL THEN CONCAT('[', Year(GetDate())-7, ':FALSE]') ELSE CONCAT('[', Year(GetDate())-7, ':TRUE]') END AS IND7
, CASE WHEN E8.PIN IS NULL THEN CONCAT('[', Year(GetDate())-8, ':FALSE]') ELSE CONCAT('[', Year(GetDate())-8, ':TRUE]') END AS IND8
, CASE WHEN E9.PIN IS NULL THEN CONCAT('[', Year(GetDate())-9, ':FALSE]') ELSE CONCAT('[', Year(GetDate())-9, ':TRUE]') END AS IND9
, CASE WHEN E10.PIN IS NULL THEN CONCAT('[', Year(GetDate())-10, ':FALSE]') ELSE CONCAT('[', Year(GetDate())-10, ':TRUE]') END AS IND10
FROM 
(SELECT DISTINCT PIN FROM AS_HEADT 
WHERE HD_CLASS IN (202, 203, 204, 205, 206, 207, 208, 209, 210, 234, 278, 295, 299, 211, 212, 213)
AND TAX_YEAR>=Year(GetDate())-10) AS T 
LEFT JOIN (SELECT PIN FROM EXEMPTIONS WHERE TAX_YEAR=Year(GetDate())-1) AS E1 ON T.PIN=E1.PIN 
LEFT JOIN (SELECT PIN FROM EXEMPTIONS WHERE TAX_YEAR=Year(GetDate())-2) AS E2 ON T.PIN=E2.PIN 
LEFT JOIN (SELECT PIN FROM EXEMPTIONS WHERE TAX_YEAR=Year(GetDate())-3) AS E3 ON T.PIN=E3.PIN 
LEFT JOIN (SELECT PIN FROM EXEMPTIONS WHERE TAX_YEAR=Year(GetDate())-4) AS E4 ON T.PIN=E4.PIN 
LEFT JOIN (SELECT PIN FROM EXEMPTIONS WHERE TAX_YEAR=Year(GetDate())-5) AS E5 ON T.PIN=E5.PIN 
LEFT JOIN (SELECT PIN FROM EXEMPTIONS WHERE TAX_YEAR=Year(GetDate())-6) AS E6 ON T.PIN=E6.PIN 
LEFT JOIN (SELECT PIN FROM EXEMPTIONS WHERE TAX_YEAR=Year(GetDate())-7) AS E7 ON T.PIN=E7.PIN
LEFT JOIN (SELECT PIN FROM EXEMPTIONS WHERE TAX_YEAR=Year(GetDate())-8) AS E8 ON T.PIN=E8.PIN
LEFT JOIN (SELECT PIN FROM EXEMPTIONS WHERE TAX_YEAR=Year(GetDate())-9) AS E9 ON T.PIN=E9.PIN
LEFT JOIN (SELECT PIN FROM EXEMPTIONS WHERE TAX_YEAR=Year(GetDate())-10) AS E10 ON T.PIN=E10.PIN
) AS X 
) AS YEARS
ON ROLL.parcel_num=YEARS.PIN

LEFT JOIN 
	( SELECT DISTINCT /* Copied VW_MOST_RECENT_IDORSALES code into this subsection to avoid leveraging a view */

	B.PIN, B.RECORDED_DATE as most_recent_sale_date, CONVERT(FLOAT, B.SALE_PRICE) as most_recent_sale_price, B.DOC_NO, B.DEED_TYPE

	FROM IDORSALES AS B

	INNER JOIN
		(SELECT DISTINCT A.PIN, A.RECORDED_DATE, A.SALE_PRICE, MAX(A.DOC_NO) AS DOC_NO FROM IDORSALES AS A
		INNER JOIN
			(SELECT X.PIN, SS.RECORDED_DATE, MAX(X.SALE_PRICE) AS SALE_PRICE FROM IDORSALES AS X
			INNER JOIN
				(SELECT PIN, MAX(RECORDED_DATE)  as RECORDED_DATE FROM IDORSALES
				WHERE DEED_TYPE NOT IN ('Q', 'E', 'B')
					AND MULT_IND = ''
				GROUP BY PIN) AS SS
				ON X.PIN=SS.PIN AND X.RECORDED_DATE=SS.RECORDED_DATE
			GROUP BY X.PIN, SS.RECORDED_DATE) AS SSS
			ON A.PIN=SSS.PIN AND A.RECORDED_DATE=SSS.RECORDED_DATE AND A.SALE_PRICE=SSS.SALE_PRICE
		GROUP BY A.PIN, A.RECORDED_DATE, A.SALE_PRICE) AS WW
		ON WW.PIN=B.PIN AND WW.RECORDED_DATE=B.RECORDED_DATE AND WW.DOC_NO=B.DOC_NO

	WHERE B.DEED_TYPE NOT IN ('Q', 'E', 'B')
		AND (B.DEED_TYPE != '' OR YEAR(B.RECORDED_DATE) <= 2000)
		) AS SALES
ON SALES.PIN=parcel_num

LEFT JOIN /* CCRD sales have the names of the property owners. */
DTBL_CCRDSALES AS CCRDSALES
ON SALES.DOC_NO=CCRDSALES.DOC_NO