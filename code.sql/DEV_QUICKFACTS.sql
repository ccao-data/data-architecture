/* 
This code produces a table of tabulated statistics to be used for various reporting needs.

FILE LAYOUT: This table is design to be flexible enough to contain different statistics at different levels of aggregation.
For example, if you are reporting for a township, you would have

[GEOGRAPHY]='Township'
GEOGRAPHY_NAME='Evanston'
GEOGRAPHY_ID=17

But if you were reporting for a commissioner district, you would have 

[GEOGRAPHY]='Cook County Commissioner District'
GEOGRAPHY_NAME='13th District'
GEOGRAPHY_ID=numeric ID

*/

/*
SELECT columns

FROM
 [DATA 1
JOIN
 DATA 2
 ON FIELDS
JOIN 
 DATA 3 .....]

WHERE specifies conditions on rows

HAVING specifies conditions on tabulated outcomes

ORDER BY is as it sounds

GROUP BY designates the tabulation groups


*/

ALTER VIEW DEV_QUICKFACTS AS

SELECT * FROM
(
SELECT 
/* Total PIN count by Township */
COUNT(HD_PIN) AS STATISTIC
, 'Total PINs' AS STATISTIC_DESCRIPTION
, 'Final BOR' AS STATISTIC_SOURCE
/* Contextual fields */
, 'Township' AS [GEOGRAPHY], township_code as GEOGRAPHY_ID, township_name as GEOGRAPHY_NAME
, TAX_YEAR AS CALENDAR_YEAR
FROM 
AS_HEADBR 
	LEFT JOIN 
	FTBL_TOWNCODES
ON LEFT(AS_HEADBR.HD_TOWN, 2)=township_code 
GROUP BY TAX_YEAR, township_code, township_name
) AS A
UNION ALL
(
/* Sum of Assessed Value by Township */
SELECT 
/* Total PIN count by Township */
 SUM(HD_ASS_BLD+HD_ASS_LND) AS STATISTIC
, 'Total Assessed Value' AS STATISTIC_DESCRIPTION
, 'Final BOR' AS STATISTIC_SOURCE
/* Contextual fields */
, 'Township' AS [GEOGRAPHY], township_code as GEOGRAPHY_ID, township_name as GEOGRAPHY_NAME
, TAX_YEAR AS CALENDAR_YEAR
FROM 
AS_HEADBR 
	LEFT JOIN 
	FTBL_TOWNCODES
ON LEFT(AS_HEADBR.HD_TOWN, 2)=township_code 
GROUP BY TAX_YEAR, township_code, township_name
)
UNION ALL
(
SELECT 
/* Total Residential PIN count by Township */
COUNT(HD_PIN) AS STATISTIC
, 'Total Residential PINs' AS STATISTIC_DESCRIPTION
, 'Final BOR' AS STATISTIC_SOURCE
/* Contextual fields */
, 'Township' AS [GEOGRAPHY], township_code as GEOGRAPHY_ID, township_name as GEOGRAPHY_NAME
, TAX_YEAR AS CALENDAR_YEAR
FROM 
AS_HEADBR 
	LEFT JOIN 
	FTBL_TOWNCODES
ON LEFT(AS_HEADBR.HD_TOWN, 2)=township_code 
WHERE LEFT(HD_CLASS,1)=2
GROUP BY TAX_YEAR, township_code, township_name
)
UNION ALL
(
SELECT 
/* Total Residential PIN count by Township */
COUNT(HD_PIN) AS STATISTIC
, 'Total Residential PINs' AS STATISTIC_DESCRIPTION
, 'Initial Assessor' AS STATISTIC_SOURCE
/* Contextual fields */
, 'Township' AS [GEOGRAPHY], township_code as GEOGRAPHY_ID, township_name as GEOGRAPHY_NAME
, TAX_YEAR AS CALENDAR_YEAR
FROM 
AS_HEADT 
	LEFT JOIN 
	FTBL_TOWNCODES
ON LEFT(AS_HEADT.HD_TOWN, 2)=township_code 
WHERE LEFT(HD_CLASS,1)=2
GROUP BY TAX_YEAR, township_code, township_name
)
UNION ALL
(
SELECT 
/* Total Residential PIN count by Township */
COUNT(HD_PIN) AS STATISTIC
, 'Total Residential PINs' AS STATISTIC_DESCRIPTION
, 'Final Assessor' AS STATISTIC_SOURCE
/* Contextual fields */
, 'Township' AS [GEOGRAPHY], township_code as GEOGRAPHY_ID, township_name as GEOGRAPHY_NAME
, TAX_YEAR AS CALENDAR_YEAR
FROM 
AS_HEADTB AS H
	LEFT JOIN 
	FTBL_TOWNCODES
ON LEFT(H.HD_TOWN, 2)=township_code 
WHERE LEFT(HD_CLASS,1)=2
GROUP BY TAX_YEAR, township_code, township_name
)
UNION ALL
(
/* Total Exemptions by township*/
SELECT STATISTIC, STATISTIC_DESCRIPTION, STATISTIC_SOURCE, 'Township' AS [GEOGRAPHY], AAB.township_code as GEOGRAPHY_ID, township_name as GEOGRAPHY_NAME, CALENDAR_YEAR FROM (
	SELECT * FROM
	(
	SELECT CALENDAR_YEAR, township_code, HO AS STATISTIC, 'Total Homeowners Exemption EAV' AS STATISTIC_DESCRIPTION, 'Exemptions' AS STATISTIC_SOURCE FROM(
		SELECT E.TAX_YEAR AS CALENDAR_YEAR, LEFT(HD_TOWN, 2) AS township_code, SUM(HO) AS HO
		FROM EXEMPTIONS AS E INNER JOIN AS_HEADT AS H ON E.TAX_YEAR=H.TAX_YEAR AND E.PIN=H.HD_PIN GROUP BY E.TAX_YEAR, LEFT(HD_TOWN, 2)) AS AA
		) AS AAA
	UNION ALL
	(
	SELECT CALENDAR_YEAR, township_code, HS AS STATISTIC, 'Total Senior Homeowners Exemption EAV' AS STATISTIC_DESCRIPTION, 'Exemptions' AS STATISTIC_SOURCE FROM(
		SELECT E.TAX_YEAR AS CALENDAR_YEAR, LEFT(HD_TOWN, 2) AS township_code, SUM(HS) AS HS
		FROM EXEMPTIONS AS E INNER JOIN AS_HEADT AS H ON E.TAX_YEAR=H.TAX_YEAR AND E.PIN=H.HD_PIN GROUP BY E.TAX_YEAR, LEFT(HD_TOWN, 2)) AS AA
		) 
	UNION ALL
	(
	SELECT CALENDAR_YEAR, township_code, SF AS STATISTIC, 'Total Senior Freeze Exemption EAV' AS STATISTIC_DESCRIPTION, 'Exemptions' AS STATISTIC_SOURCE FROM(
		SELECT E.TAX_YEAR AS CALENDAR_YEAR, LEFT(HD_TOWN, 2) AS township_code, SUM(SF) AS SF
		FROM EXEMPTIONS AS E INNER JOIN AS_HEADT AS H ON E.TAX_YEAR=H.TAX_YEAR AND E.PIN=H.HD_PIN GROUP BY E.TAX_YEAR, LEFT(HD_TOWN, 2)) AS AA
		) 
    UNION ALL
	(
	SELECT CALENDAR_YEAR, township_code, VETERAN AS STATISTIC, 'Total Veterans Exemption EAV' AS STATISTIC_DESCRIPTION, 'Exemptions' AS STATISTIC_SOURCE FROM(
		SELECT E.TAX_YEAR AS CALENDAR_YEAR, LEFT(HD_TOWN, 2) AS township_code, SUM(VETERAN) AS VETERAN 
		FROM EXEMPTIONS AS E INNER JOIN AS_HEADT AS H ON E.TAX_YEAR=H.TAX_YEAR AND E.PIN=H.HD_PIN GROUP BY E.TAX_YEAR, LEFT(HD_TOWN, 2)) AS AA
		) 
   UNION ALL
	(
	SELECT CALENDAR_YEAR, township_code, RETURN_VET AS STATISTIC, 'Total Returning Veterans Exemption EAV' AS STATISTIC_DESCRIPTION, 'Exemptions' AS STATISTIC_SOURCE FROM(
		SELECT E.TAX_YEAR AS CALENDAR_YEAR, LEFT(HD_TOWN, 2) AS township_code, SUM(RETURN_VET) AS RETURN_VET, SUM(DVET_LT75+DVET_GE75) AS DVET
		FROM EXEMPTIONS AS E INNER JOIN AS_HEADT AS H ON E.TAX_YEAR=H.TAX_YEAR AND E.PIN=H.HD_PIN GROUP BY E.TAX_YEAR, LEFT(HD_TOWN, 2)) AS AA
		) 
   UNION ALL
	(
	SELECT CALENDAR_YEAR, township_code, DVET AS STATISTIC, 'Total Disabled Veterans Exemption EAV' AS STATISTIC_DESCRIPTION, 'Exemptions' AS STATISTIC_SOURCE FROM(
		SELECT E.TAX_YEAR AS CALENDAR_YEAR, LEFT(HD_TOWN, 2) AS township_code, SUM(DVET_LT75+DVET_GE75) AS DVET
		FROM EXEMPTIONS AS E INNER JOIN AS_HEADT AS H ON E.TAX_YEAR=H.TAX_YEAR AND E.PIN=H.HD_PIN GROUP BY E.TAX_YEAR, LEFT(HD_TOWN, 2)) AS AA
		) 
		) AS AAB
	LEFT JOIN 
	FTBL_TOWNCODES AS FTBL
ON AAB.township_code=FTBL.township_code 
)
/*ORDER BY CALENDAR_YEAR, [GEOGRAPHY], STATISTIC_DESCRIPTION*/