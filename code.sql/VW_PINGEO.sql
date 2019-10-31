ALTER VIEW VW_PINGEO AS

SELECT DISTINCT

PL_PIN AS PIN
/* PROPLOCS VARIABLES */
, LTRIM(RTRIM(CAST(PL_HOUSE_NO AS varchar(10)))) + ' '
+ LTRIM(RTRIM(PL_DIR)) + ' '
+ LTRIM(RTRIM(PL_STR_NAME)) + ' '
+ LTRIM(RTRIM(PL_STR_SUFFIX)) AS PROPERTY_ADDRESS
, PL_APT_NO AS PROPERTY_APT_NO
, PL_CITY_NAME AS PROPERTY_CITY
, PL_ZIPCODE AS PROPERTY_ZIP
/* HEAD VARIABLES */
, HD_ADDR AS MAILING_ADDRESS, HD_CITY  AS MAILING_CITY, HD_ZIP AS MAILING_ZIP
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
, centroid_x, centroid_y
/* HEAD VARIABLES */
, TAX_YEAR AS most_recent
/* JOIN KEY */
, CASE WHEN LEN(CAST(HD_NBHD AS varchar)) = 2 THEN CAST(PL_TOWN AS varchar) + '0' + CAST(HD_NBHD AS varchar)
ELSE CAST(PL_TOWN AS varchar) + CAST(HD_NBHD AS varchar) END AS [town_nbhd.KEY]

FROM PROPLOCS AS prop

INNER JOIN
	(SELECT * FROM
	(SELECT *, ROW_NUMBER() OVER(PARTITION BY HD_PIN ORDER BY TAX_YEAR DESC) AS rn FROM AS_HEADT) a
	WHERE rn = 1) h
	ON h.PIN = prop.PL_PIN
INNER JOIN
	(SELECT * FROM PINLOCATIONS WHERE primary_polygon = 1 AND PIN999 != 1) lox
	ON LEFT(prop.PL_PIN, 10) = LEFT(lox.Name, 10)
LEFT JOIN
	(SELECT township_code, township_name FROM FTBL_TOWNCODES) town
	on prop.PL_TOWN = town.township_code