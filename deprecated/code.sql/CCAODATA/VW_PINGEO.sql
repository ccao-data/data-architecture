ALTER VIEW VW_PINGEO AS

/* 
The goal of this view is to provide geographic information about all PINs. It
combines the spatial location data stored in DTBL_PINLOCATIONS with the address
information located in PROPLOCS

It also cleans up various issues in the PROPLOCS table, such as whitespace and
formatting issues in addresses
*/

/*
CTE to get the latest mailing address from HEADT (gets most recent year PIN exists)
*/
WITH HEADT AS (
	SELECT 
		T.PIN, 
		T.TAX_YEAR, 
		T.HD_ADDR, 
		T.HD_CITY, 
		T.HD_ZIP, 
		T.HD_NBHD
	FROM AS_HEADT T
	INNER JOIN (
		SELECT PIN, MAX(TAX_YEAR) AS max_year
		FROM AS_HEADT
		GROUP BY PIN
	) A
	ON T.PIN = A.PIN AND T.TAX_YEAR = A.max_year
)

SELECT DISTINCT
	CASE WHEN PL_PIN IS NULL THEN pinlocs.PIN 
		ELSE PL_PIN 
		END AS PIN,

	/* PROPLOCS variables. These are the actual physical address of the property */
	NULLIF(CONCAT(
		LTRIM(RTRIM(CAST(PL_HOUSE_NO AS varchar(10)))), ' ',
		LTRIM(RTRIM(PL_DIR)), ' ',
		LTRIM(RTRIM(PL_STR_NAME)), ' ',
		LTRIM(RTRIM(PL_STR_SUFFIX))
	), '   ') AS PROPERTY_ADDRESS,
	NULLIF(PL_APT_NO, '') AS PROPERTY_APT_NO,
	NULLIF(LTRIM(RTRIM(PL_CITY_NAME)), '') AS PROPERTY_CITY,
	NULLIF(CONCAT(LEFT(PL_ZIPCODE, 5), '-', RIGHT(PL_ZIPCODE, 4)), '-') AS PROPERTY_ZIP,

	/* HEAD variables. These are the mailing address of the property owner */
	NULLIF(RIGHT(LTRIM(RTRIM(HD_CITY)), 2), '') AS MAILING_STATE,
	NULLIF(HD_ADDR, '') AS MAILING_ADDRESS,
	CASE WHEN LEN(LTRIM(RTRIM(HD_CITY))) >= 2 THEN NULLIF(LTRIM(RTRIM(LEFT(LTRIM(RTRIM(HD_CITY)), LEN(LTRIM(RTRIM(HD_CITY))) - 2))), '')
		ELSE NULLIF(HD_CITY, '')
		END AS MAILING_CITY,
	NULLIF(HD_ZIP, 0) AS MAILING_ZIP,

	/* PINLOCATIONS variables */
	CONVERT(FLOAT, centroid_x) AS centroid_x,
	CONVERT(FLOAT, centroid_y) AS centroid_y,
	GEOID AS geoid,
	TRACTCE AS census_tract,

	/* Census-derived variables */
	tract_pop,
	white_perc,
	black_perc,
	asian_perc,
	his_perc,
	other_perc,
	midincome,
	PUMA,

	/* Political boundaries */
	FIPS,
	municipality,
	commissioner_dist,
	reps_dist,
	senate_dist,
	ward,
	ssa_name,
	ssa_no,
	tif_agencynum,

	/* Environment variables */
	ohare_noise,
	floodplain,
	fs_flood_factor,
	fs_flood_risk_direction,
	withinmr100,
	withinmr101300,
	school_elem_district,
	school_hs_district,

	/* Other variables */
	PL_TOWN AS township,
	HD_NBHD AS nbhd,
	township_name,
	TAX_YEAR AS most_recent,

	/* Indicators for which data is not missing */
	CASE WHEN PL_PIN IS NOT NULL THEN 1 
		ELSE 0 
		END AS PROPLOCS,
	CASE WHEN pinlocs.PIN IS NOT NULL THEN 1 
		ELSE 0 
		END AS PINLOCATIONS
FROM PROPLOCS AS prop

INNER JOIN HEADT 
ON HEADT.PIN = prop.PL_PIN

/* Join property addresses and locations together to get the full set of propertys.
Using LEFT in the join field here is the source of slowness but is difficult to
eliminate due to the nature of the data/join */
FULL JOIN (
	SELECT *
	FROM DTBL_PINLOCATIONS
	WHERE primary_polygon = 1 
	AND PIN999 != 1
) pinlocs
ON LEFT(prop.PL_PIN, 10) = LEFT(pinlocs.PIN, 10)

LEFT JOIN (
	SELECT township_code, township_name
	FROM FTBL_TOWNCODES
) town
ON prop.PL_TOWN = town.township_code
