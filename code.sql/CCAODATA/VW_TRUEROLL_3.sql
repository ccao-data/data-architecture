/* This query returns properties that received a certificate of error for a missing residential exemption */


SELECT PIN, 2000+COE_TAX_YR AS CALENDAR_YEAR, COE_WC_NAME AS NAME 
FROM AS_RES_CERTOFCORRECTIONS AS C 
WHERE (COE_ACT_TYPE IN (01,02,06, 20) 
AND COE_REASON IN (01, 02, 43, 44, 56, 62, 63, 64, 65, 66,
67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 
82, 73, 84, 85, 86, 97)) AND COE_TAX_YR<=20