ALTER VIEW VW_OPENDATA_ASSESSMENTS AS

/*
CCAO has a weird data architecture. HEADT contains the mailed assessed values for each PIN.
However, during the current year, there are no records for PINs before their assessments are mailed.
This presents the challenge of reporting on values without a stable definition of the universe of PINs.
This query is constructed so that it pulls PIN and YEAR from either HEAD

Below is a CTE defining the possible combinations of year and PIN. Goal is to get the full universe of
PIN and TAX_YEAR combinations, even in cases where that combination doesn't yet exist in the data
*/
WITH MAX_YEAR AS (
	SELECT MAX(TAX_YEAR) AS YEAR
	FROM AS_HEADT
),
MOST_RECENT_YEAR_FOR_EACH_PIN AS (
	SELECT PIN, 
		(SELECT YEAR FROM MAX_YEAR) AS TAX_YEAR,
		MAX(TAX_YEAR) AS REAL_YEAR
	FROM AS_HEADT
	WHERE TAX_YEAR >= (SELECT YEAR FROM MAX_YEAR) - 1
	GROUP BY PIN
),
PIN_UNIVERSE AS (
	SELECT PIN, TAX_YEAR, LEFT(HD_TOWN, 2) AS TOWN, HD_NBHD AS NBHD, HD_CLASS AS CLASS
	FROM AS_HEADT
	WHERE TAX_YEAR < (SELECT YEAR FROM MAX_YEAR)
	UNION
	/* The goal of this subquery is to create a data row for the current year, even if that row doesn't yet exist in AS_HEADT.
	To do this, we combine the latest year's data with the previous year's data. If the current year's data is different from
	the previous year's, we take the current year. Otherwise, we simply drop duplicates (with UNION) to combine the two years.
	This subquery should always return a single row.
	*/
	SELECT a.PIN, b.TAX_YEAR, LEFT(HD_TOWN, 2) AS TOWN, HD_NBHD AS NBHD, HD_CLASS AS CLASS
	FROM AS_HEADT a
	INNER JOIN MOST_RECENT_YEAR_FOR_EACH_PIN b 
	ON a.PIN = b.PIN AND a.TAX_YEAR = b.REAL_YEAR
)
SELECT
	PIN_UNIVERSE.PIN,
	PIN_UNIVERSE.TAX_YEAR AS [YEAR],
	PIN_UNIVERSE.CLASS,
	PIN_UNIVERSE.NBHD,
	PIN_UNIVERSE.TOWN,
	towns.township_name AS [TOWN NAME],
	CL_TXCD as [TAX CODE],
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
		CLASS,
		fitted_value_1 AS [MODEL RESULT],
		fitted_value_6 AS [PIPELINE RESULT],
		CASE WHEN version IS NULL THEN 0
			 ELSE version
			 END as VERSION
	FROM DTBL_MODELVALS
	WHERE max_version = 1
) AS FITTED_VALUES
ON PIN_UNIVERSE.PIN = FITTED_VALUES.PIN 
AND PIN_UNIVERSE.TAX_YEAR = FITTED_VALUES.YEAR
AND PIN_UNIVERSE.CLASS = FITTED_VALUES.CLASS

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

-- Merge taxcodes
LEFT JOIN
CLERKVALUES
ON CLERKVALUES.PIN =  PIN_UNIVERSE.PIN  AND CLERKVALUES.TAX_YEAR = PIN_UNIVERSE.TAX_YEAR