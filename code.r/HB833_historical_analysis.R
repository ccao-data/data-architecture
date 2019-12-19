

# Analysis of Senior Homestead Exemptions

rm(list = ls(all.names = TRUE))

# Load utilities
source(paste0("C:/Users/", Sys.info()[['user']],"/Documents/ccao_utility/code.r/99_utility_2.r"))
invisible(check.packages(libs))
dirs <- directories("data-architecture")
CCAODATA <- dbConnect(odbc()
                      , driver   = "SQL Server"
                      , server   = odbc.credentials("server")
                      , database = odbc.credentials("database")
                      , uid      = odbc.credentials("uid")
                      , pwd      = odbc.credentials("pwd"))

# Summary statistics and data integrity
# Here, I just want to check each section of the larger view to make sure we have accurate row counts.
hb833 <- dbGetQuery(CCAODATA, paste0("
SELECT E.*, LTRIM(RTRIM(CAST(PL_HOUSE_NO AS varchar(10)))) + ' '
+ LTRIM(RTRIM(PL_DIR)) + ' '
+ LTRIM(RTRIM(PL_STR_NAME)) + ' '
+ LTRIM(RTRIM(PL_STR_SUFFIX)) AS PROPERTY_ADDRESS
, PL_CITY_NAME AS PROPERTY_CITY
, PL_ZIPCODE AS PROPERTY_ZIP
	, CASE WHEN K.LEX_DEATH_ID IS NULL THEN 0 ELSE 1 END AS LEXIS_DECEASED 
	, CASE WHEN YEAR(J.LEX_DEATHDATE) IS NULL THEN 0 ELSE 1 END AS LEXIS_DEATH 
	, CASE WHEN M.DEATH_DATE IS NULL THEN 0 ELSE 1 END AS IDPH_DECEASED 
	, CASE WHEN YEAR(L.DEATH_DATE) IS NULL THEN 0 ELSE 1 END AS IDPH_DEATH
	, CASE WHEN CCRD_SALES IS NULL THEN 0 ELSE CCRD_SALES END AS CCRD_SALES
	, CASE WHEN IDOR_SALES IS NULL THEN 0 ELSE IDOR_SALES END AS IDOR_SALES
	FROM(	
		/* PART 1 - a dataset unique by PIN, Name, and birthday to join flag data against */
		/* In this section, we join SENIOREXEMPTIONS against itself, lagged one year, to get deltas */
		SELECT D.CALENDAR_YEAR, D.PIN, LTRIM(RTRIM(D.[NAME])) AS TAXPAYER_NAME
		, D.BIRTH_DATE AS BIRTH_DATE
		, CASE WHEN D.CALENDAR_YEAR>=(SELECT MAX(TAX_YEAR)-1 FROM SENIOREXEMPTIONS) THEN NULL ELSE D.STATUS_CHANGE_NEXTYEAR END AS STATUS_CHANGE_NEXTYEAR
		, SF AS SENIOR_FREEZE
		, YEARS_ON_TOTAL, FINAL_YEAR
		/* This inner query creates a data set with Year, Name, PIN and a status change indicator by joining exemption data against itself, and then also accounting for C of Es */
		FROM (SELECT 
				CASE WHEN A.TAX_YEAR IS NULL AND B.TAX_YEAR_LEAD IS NOT NULL THEN B.TAX_YEAR_LEAD 
				WHEN A.TAX_YEAR IS NOT NULL AND B.TAX_YEAR_LEAD IS NULL THEN A.TAX_YEAR 
				WHEN A.TAX_YEAR IS NOT NULL AND B.TAX_YEAR_LEAD IS NOT NULL THEN A.TAX_YEAR
				WHEN A.TAX_YEAR IS NULL AND B.TAX_YEAR_LEAD IS NULL THEN COE.CALENDAR_YEAR
					END AS CALENDAR_YEAR
			  , CASE WHEN A.[NAME] IS NULL AND B.[NAME] IS NOT NULL THEN B.[NAME] 
				WHEN A.[NAME] IS NOT NULL AND B.[NAME] IS NULL THEN A.[NAME]
				WHEN A.[NAME] IS NOT NULL AND B.[NAME] IS NOT NULL THEN A.[NAME]
				WHEN A.[NAME] IS NULL AND B.[NAME] IS NULL THEN COE.[NAME] 
					END AS NAME
			  , CASE WHEN A.BIRTH_DATE IS NULL AND  B.BIRTH_DATE IS NOT NULL THEN B.BIRTH_DATE 
			  WHEN A.BIRTH_DATE IS NOT NULL AND  B.BIRTH_DATE IS NULL THEN A.BIRTH_DATE 
			  WHEN A.BIRTH_DATE IS NOT NULL AND  B.BIRTH_DATE IS NOT NULL THEN A.BIRTH_DATE 
			  WHEN A.BIRTH_DATE IS NULL AND  B.BIRTH_DATE IS NULL THEN NULL
					END AS BIRTH_DATE
			  , CASE WHEN A.PIN IS NULL AND  B.PIN IS NOT NULL THEN B.PIN 
			  WHEN A.PIN IS NOT NULL AND  B.PIN IS NULL THEN A.PIN 
			  WHEN A.PIN IS NOT NULL AND  B.PIN IS NOT NULL THEN A.PIN 
			  WHEN A.PIN IS NULL AND  B.PIN IS NULL THEN COE.PIN 
					END AS PIN
			  /* This is the delta column showing the change from the previous year */ 
			  , CASE  /*Leave roll, no COE */
			  WHEN B.TAX_YEAR_LEAD IS NULL /* Not on roll for following year */
					AND A.TAX_YEAR IS NOT NULL /* On roll for current year */
					AND COE2.LEAD_CALENDAR_YEAR IS NULL /* Did not get a COE in the following year */
						THEN -1 /* They will leave */
				WHEN /* Leave roll, get COE next year */
					B.TAX_YEAR_LEAD IS NULL /* Not on roll for following year */
					AND A.TAX_YEAR IS NOT NULL /* On roll for current year */
					AND COE2.LEAD_CALENDAR_YEAR IS NOT NULL /* Did get a COE in the following year */
						THEN 0 /* No Change */
				WHEN /* Enter roll, no COE in prior year */
				A.TAX_YEAR IS NULL /* Not on roll for current year */
				AND B.TAX_YEAR_LEAD IS NOT NULL /* Are on roll for following year */
				AND COE.CALENDAR_YEAR IS NULL /* No COE in current year */
						THEN 1 /* They will enter */
				WHEN /* Enter roll, yes COE in prior year */
				A.TAX_YEAR IS NULL /* Not on roll for current year */
				AND B.TAX_YEAR_LEAD IS NOT NULL /* Are on roll for following year */
				AND COE.CALENDAR_YEAR IS NOT NULL /* Have a COE in current year */
						THEN 0 /* No Change */
				WHEN /* Don't leave roll */ 
					B.TAX_YEAR_LEAD IS NOT NULL 
					AND A.TAX_YEAR IS NOT NULL 
						THEN 0 /* No change*/
			END AS STATUS_CHANGE_NEXTYEAR 
				/* indicate whether they received a COE in the current year */
				, CASE WHEN COE.CALENDAR_YEAR IS NOT NULL THEN 1 ELSE 0 END AS COE
			FROM 
				/* This join allows for the delta calculation the change from the prior year */
				(SELECT NAME, BIRTH_DATE, PIN, TAX_YEAR FROM SENIOREXEMPTIONS) AS A	
				FULL OUTER JOIN
				(SELECT NAME, BIRTH_DATE, PIN, TAX_YEAR-1 AS TAX_YEAR_LEAD FROM SENIOREXEMPTIONS) AS B
				ON A.NAME=B.NAME AND A.BIRTH_DATE=B.BIRTH_DATE AND A.TAX_YEAR=B.TAX_YEAR_LEAD AND A.PIN=B.PIN
				/* Need to account for certificates of error */
				LEFT JOIN
				(SELECT PIN, 2000+COE_TAX_YR AS CALENDAR_YEAR, COE_WC_NAME AS NAME 
				FROM AS_RES_CERTOFCORRECTIONS AS C 
				WHERE (COE_ACT_TYPE IN (4) OR (COE_ACT_TYPE IN (20) AND COE_REASON IN (43, 44, 84, 85, 86))) AND COE_TAX_YR<=20) AS COE
				ON A.PIN=COE.PIN AND A.TAX_YEAR=COE.CALENDAR_YEAR
				LEFT JOIN
				(SELECT PIN, 2000+COE_TAX_YR-1 AS LEAD_CALENDAR_YEAR FROM AS_RES_CERTOFCORRECTIONS AS C 
				WHERE (COE_ACT_TYPE IN (4) OR (COE_ACT_TYPE IN (20) AND COE_REASON IN (43, 44, 84, 85, 86))) AND COE_TAX_YR<=20) AS COE2
				ON A.PIN=COE2.PIN AND A.TAX_YEAR=COE2.LEAD_CALENDAR_YEAR
				) AS D 
			/* This gets some additional contextual information at the individual level, how many years they have an exemption, and what their last year is*/
			LEFT JOIN
			(SELECT COUNT(NAME) AS YEARS_ON_TOTAL, MAX(TAX_YEAR) AS FINAL_YEAR, NAME, BIRTH_DATE, PIN FROM SENIOREXEMPTIONS GROUP BY PIN, NAME, BIRTH_DATE) AS C
			ON D.PIN=C.PIN AND D.NAME=C.NAME AND D.BIRTH_DATE=C.BIRTH_DATE /* This may create data errors where a senior changed PINs */
			/* This provides context about what exemptions they received */
			INNER JOIN /* Need an INNER JOIN to account for people who moved between homes */
			(SELECT PIN, TAX_YEAR AS CALENDAR_YEAR, HS, SF FROM EXEMPTIONS WHERE HS>0) AS H
			ON H.PIN=D.PIN AND H.CALENDAR_YEAR=D.CALENDAR_YEAR 
			WHERE (1=1)
			/* The last year in the data set will give you issues if this is run midyear */
			AND D.CALENDAR_YEAR<(SELECT MAX(TAX_YEAR) FROM SENIOREXEMPTIONS) AND D.CALENDAR_YEAR>=2008
		) AS E
	/* Need address field for death record matching */
	INNER JOIN
		PROPLOCS 
	ON E.PIN = PL_PIN
	LEFT JOIN
		/* PART 2 - sales flages. This should be a 1:1 corrosponance */
		(SELECT DISTINCT PIN, COUNT(DISTINCT DOC_NO) AS CCRD_SALES, YEAR(EXECUTED_DATE) AS SALE_YEAR FROM DTBL_CCRDSALES GROUP BY PIN, YEAR(EXECUTED_DATE)) AS F
	ON E.CALENDAR_YEAR=F.SALE_YEAR AND E.PIN=F.PIN
	LEFT JOIN
		(SELECT DISTINCT PIN, COUNT(DISTINCT DOC_NO) AS IDOR_SALES, YEAR(RECORDED_DATE) AS SALE_YEAR FROM IDORSALES GROUP BY PIN, YEAR(RECORDED_DATE)) AS G
	ON E.CALENDAR_YEAR=G.SALE_YEAR AND E.PIN=G.PIN
		/* Part 3 - death flags */
		/* LEXISNEXIS data */
		/* Flag a death in the year of death */
	LEFT JOIN
		(SELECT * FROM DTBL_LEXISNEXIS_SENIOR_DEATHS WHERE LEX_DEATH_ID NOT IN ('')) AS J
	ON E.CALENDAR_YEAR=YEAR(LEX_DEATHDATE) AND E.PIN=J.PIN AND E.TAXPAYER_NAME=J.NAME AND E.BIRTH_DATE=J.BIRTH_DAY
	LEFT JOIN
		/* Indicate whether someone is deceased in any year */
	(SELECT * FROM DTBL_LEXISNEXIS_SENIOR_DEATHS WHERE LEX_DEATH_ID NOT IN ('')) AS K
	ON E.TAXPAYER_NAME=K.NAME AND E.BIRTH_DATE=K.BIRTH_DAY AND E.PIN=K.PIN 
		/*IDPH data */
		/* Flag a death in the year of death */
	LEFT JOIN
	DTBL_IDPH_SENIOR_DEATHS AS L
		ON E.TAXPAYER_NAME=CONCAT(L.LAST_NAME, ' ', L.FIRST_NAME, ' ', L.MIDDLE_NAME) 
		AND E.BIRTH_DATE=L.BIRTH_DATE 
		AND E.CALENDAR_YEAR=YEAR(DEATH_DATE) 
		AND LTRIM(RTRIM(CAST(PL_HOUSE_NO AS varchar(10)))) + ' '
+ LTRIM(RTRIM(PL_DIR)) + ' '
+ LTRIM(RTRIM(PL_STR_NAME)) + ' '
+ LTRIM(RTRIM(PL_STR_SUFFIX))=L.[RESIDENCE ADDRESS] 
		AND PL_CITY_NAME=L.[RESIDENCE CITY] 
		AND CONVERT(VARCHAR, PL_ZIPCODE)=L.[RESIDENCE ZIP] 
		/* Indicate whether someone is deceased at all */
	LEFT JOIN
	 DTBL_IDPH_SENIOR_DEATHS AS M
		ON E.TAXPAYER_NAME=CONCAT(M.LAST_NAME, ' ', M.FIRST_NAME, ' ', M.MIDDLE_NAME)  
		AND E.BIRTH_DATE=M.BIRTH_DATE AND LTRIM(RTRIM(CAST(PL_HOUSE_NO AS varchar(10)))) + ' '
+ LTRIM(RTRIM(PL_DIR)) + ' '
+ LTRIM(RTRIM(PL_STR_NAME)) + ' '
+ LTRIM(RTRIM(PL_STR_SUFFIX))=M.[RESIDENCE ADDRESS] 
		AND PL_CITY_NAME=M.[RESIDENCE CITY] 
		AND CONVERT(VARCHAR, PL_ZIPCODE)=M.[RESIDENCE ZIP] 		
ORDER BY CALENDAR_YEAR, PIN
	"))

table <- subset(hb833, CALENDAR_YEAR>2010 & !is.na(STATUS_CHANGE_NEXTYEAR)) %>%
  group_by(STATUS_CHANGE_NEXTYEAR, CALENDAR_YEAR) %>%
  summarise(DEATHS = sum(LEXIS_DEATH, IDPH_DEATH)
            , SALES = sum(CCRD_SALES, IDOR_SALES), n = n())
table$rate <- (table$DEATHS+table$SALES)/table$n
table[table$CALENDAR_YEAR==2017,]

