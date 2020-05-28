ALTER VIEW VW_OPENDATA_ASSESSMENTS AS

/*
CCAO has a weird data architecture. HEADT contains the mailed assessed values for each PIN.
However, during the current year, there are no records for PINs before their assessments are mailed.
This presents the challenge of reporting on values without a stable definition of the universe of PINs.
This query is constructed so that it pulls PIN and YEAR from either HEAD

Below is a CTE defining the possible combinations of year and PIN. Goal is to get the full set
of possible PINs for the most recent year of data, even if no data yet exists to
populate those rows. This is accomplished by directly grabbing all PIN data for
all years <= max_year - 1, and then combining the PIN data of max_year and max_year - 1
to get a (mostly) up-to-date set
*/
WITH PIN_UNIVERSE AS
(
	SELECT PIN, TAX_YEAR, LEFT(HD_TOWN, 2) AS TOWN, HD_NBHD AS NBHD, HD_CLASS AS CLASS
	FROM AS_HEADT
	WHERE TAX_YEAR <= (SELECT MAX(TAX_YEAR) FROM AS_HEADT) - 1
	UNION
	SELECT PIN, TAX_YEAR, TOWN, NBHD, CLASS
	FROM (
		SELECT PIN, TAX_YEAR, LEFT(HD_TOWN, 2) AS TOWN, HD_NBHD AS NBHD, HD_CLASS AS CLASS
		FROM AS_HEADT
		WHERE TAX_YEAR = (SELECT MAX(TAX_YEAR) FROM AS_HEADT)
		UNION
		SELECT PIN, TAX_YEAR + 1 AS TAX_YEAR, LEFT(HD_TOWN, 2) AS TOWN, HD_NBHD AS NBHD, HD_CLASS AS CLASS
		FROM AS_HEADT
		WHERE TAX_YEAR = (SELECT MAX(TAX_YEAR) FROM AS_HEADT) - 1
	) a
	WHERE CLASS IN (200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 234, 241, 278, 295, 299)
)
SELECT
	PIN_UNIVERSE.PIN,
	PIN_UNIVERSE.TAX_YEAR AS [YEAR],
	PIN_UNIVERSE.CLASS,
	PIN_UNIVERSE.NBHD,
	PIN_UNIVERSE.TOWN,
	towns.township_name AS [TOWN NAME],
	[MODEL RESULT],
	[PIPELINE RESULT],
	(HEADT.HD_ASS_BLD + HEADT.HD_ASS_LND) * 10 AS [FIRST PASS],
	[CERTIFIED],
	(BOR.HD_ASS_LND + BOR.HD_ASS_BLD) * 10 AS [BOR RESULT],
	CASE WHEN [APPEALED] = 1 THEN 'YES'
		 ELSE 'NO'
		 END AS [APPEALED],
	[NUM REVIEWED],
	CASE WHEN [CHANGED] = 1 THEN 'YES'
		 WHEN [CHANGED] = 0 THEN 'NO'
		 ELSE NULL
		 END AS [CHANGED]
FROM PIN_UNIVERSE

--- Merge township names
LEFT JOIN (
	SELECT township_name, township_code
	FROM FTBL_TOWNCODES
) towns
ON PIN_UNIVERSE.TOWN = towns.township_code

--- Merge model and pipeline values, need to make sure only values from the latest pipeline version are used
LEFT JOIN (
	SELECT
		PIN,
		TAX_YEAR AS YEAR,
		fitted_value_1 AS [MODEL RESULT],
		fitted_value_6 AS [PIPELINE RESULT],
		CASE WHEN version IS NULL THEN 0
			 ELSE version
			 END as VERSION
	FROM DTBL_MODELVALS
	WHERE max_version = 1
) AS FITTED_VALUES
ON PIN_UNIVERSE.PIN = FITTED_VALUES.PIN AND PIN_UNIVERSE.TAX_YEAR = FITTED_VALUES.YEAR

--- Merge mailed (first pass) values from AS_HEADT
LEFT JOIN AS_HEADT AS HEADT
ON PIN_UNIVERSE.PIN = HEADT.PIN AND PIN_UNIVERSE.TAX_YEAR = HEADT.TAX_YEAR

--- Merge assessor certified (second pass / post-mailing values)
LEFT JOIN (
	SELECT PIN, (HD_ASS_BLD + HD_ASS_LND) * 10 AS CERTIFIED, TAX_YEAR
	FROM AS_HEADTB
) AS HEADTB
ON PIN_UNIVERSE.PIN = HEADTB.PIN AND PIN_UNIVERSE.TAX_YEAR = HEADTB.TAX_YEAR

--- Merge Board of Review values
LEFT JOIN AS_HEADBR AS BOR
ON PIN_UNIVERSE.PIN = BOR.PIN AND PIN_UNIVERSE.TAX_YEAR = BOR.TAX_YEAR

--- Merge data on apppeals
LEFT JOIN (
	SELECT
		PIN,
		CASE WHEN PC_PIN_RESULT_1 = 'C'
			 OR PC_PIN_RESULT_2 = 'C'
			 OR PC_PIN_RESULT_3 = 'C' THEN 1
			 ELSE 0
			 END AS [CHANGED],
		CASE WHEN PC_PIN_RESULT_3 != ''
			 AND PC_PIN_RESULT_2 = '' THEN 1
			 WHEN PC_PIN_RESULT_2 != ''
			 AND PC_PIN_RESULT_1 = '' THEN 2
			 WHEN PC_PIN_RESULT_1 != '' THEN 3
			 ELSE 0
			 END AS [NUM REVIEWED],
		1 AS [APPEALED], TAX_YEAR
	FROM APPEALSDATA
) AS APPEALS
ON PIN_UNIVERSE.PIN = APPEALS.PIN AND PIN_UNIVERSE.TAX_YEAR = APPEALS.TAX_YEAR
