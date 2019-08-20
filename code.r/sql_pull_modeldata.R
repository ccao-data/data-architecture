modeldata <- dbGetQuery(CCAODATA, paste0("
SELECT
/* Fields from HEAD */
  HD_PIN as PIN, HD_CLASS as CLASS, H.TAX_YEAR, HD_NBHD AS NBHD, HD_HD_SF AS HD_SF
, CONVERT(INT, SUBSTRING(CONVERT(CHARACTER, HD_TOWN),1,2)) as TOWN_CODE
/* Fields from CCAOSFCHARS */
  , CASE WHEN TYPE_RESD=0 THEN NULL ELSE TYPE_RESD END AS TYPE_RESD /* NULL recorded as 0 in AS400 */
    , APTS, EXT_WALL, ROOF_CNST, ROOMS, BEDS, BSMT, BSMT_FIN, HEAT, OHEAT, AIR, FRPL, ATTIC_TYPE
  , CASE WHEN ATTIC_FNSH=0 THEN NULL ELSE ATTIC_FNSH END AS ATTIC_FNSH /* NULL recorded as 0 in AS400 */
    , HBATH, TP_PLAN, TP_DSGN, CNST_QLTY
  , CASE WHEN RENOVATION=0 THEN NULL ELSE RENOVATION END AS RENOVATION /* NULL recorded as 0 in AS400 */
    , SITE, GAR1_SIZE, GAR1_CNST, GAR1_ATT, GAR1_AREA, GAR2_SIZE, GAR2_CNST, GAR2_ATT
  , GAR2_AREA
  , CASE WHEN PORCH=0 THEN NULL ELSE PORCH END AS PORCH /* NULL recorded as 0 in AS400 */
    , OT_IMPR, BLDG_SF, REPAIR_CND, MULTI_CODE, VOLUME, NCU
  /* Fields from BRMASTER, Board of Review final figures */
    , (LANDVAL)*10 as PRI_EST_LAND, (BLDGVAL)*10 as PRI_EST_BLDG
  /* Fields from IDORSALES */
    , sale_date, DOC_NO, sale_price
  /* Location data from PINGEO */
    , centroid_x, centroid_y, TRACTCE, ohare_noise, floodplain, withinmr100, withinmr101300, PUMA
  /* face sheet CDUS from detail */
    , DT_CDU AS CDU
  /* strata */
  /*, condo_strata_10, condo_strata_100*/
    , condo_strata
  /* Calculated field from CCAOSFCHARS where MULTI_IND==1 */
    , total_bldg_sf
  /* Recoded fields */
    , 1 as cons
  , CASE WHEN MULTI_IND IS NULL THEN 0 ELSE MULTI_IND END AS MULTI_IND
  /* Account for missing addresses, then construct convenient property address field. */
    , CASE WHEN PL_HOUSE_NO IS NULL THEN 'ADDRESS MISSING FROM PROPLOCS' ELSE
  LTRIM(RTRIM(CAST(PL_HOUSE_NO as varchar(10)))) + ' ' + LTRIM(RTRIM(PL_DIR))+ ' ' + LTRIM(RTRIM(PL_STR_NAME))+
    ' ' + LTRIM(RTRIM(PL_STR_SUFFIX)) + ' ' + LTRIM(RTRIM(PL_CITY_NAME)) END as ADDR
  /* Assign properties to modeling groups */
    , CASE WHEN HD_CLASS IN (200, 201, 241) OR (HD_CLASS = 299 AND (DT_CDU != 'GR' OR DT_CDU IS NULL) AND (BLDGVAL + LANDVAL) > 10) THEN 'NCHARS'
  WHEN HD_CLASS IN (202, 203, 204, 205, 206, 207, 208, 209, 210, 234, 278, 295) THEN 'SF'
  WHEN HD_CLASS IN (211, 212) THEN 'MF'
  WHEN HD_CLASS = 299 AND (DT_CDU = 'GR' OR (BLDGVAL + LANDVAL) <= 10) THEN 'FIXED'
  ELSE NULL END AS modeling_group
  /* Account for missing characteristics for NCHARS group */
    , CASE WHEN FBATH IS NULL THEN 1 ELSE FBATH END AS FBATH
  , CASE WHEN HD_CLASS = 299 AND NCHARS_AGE IS NOT NULL THEN NCHARS_AGE
  WHEN AGE IS NULL THEN 10 ELSE AGE END AS AGE
  , CASE WHEN [USE] IS NULL THEN 1 ELSE [USE] END AS [USE]
  /* Count number of condo units in a building */
    , n_units
  /* Where we have missing percentage of ownership, we divide values equally */
    , CASE WHEN DT_PER_ASS=0 THEN 1/n_units
  WHEN DT_PER_ASS IS NULL THEN 1/n_units
  ELSE DT_PER_ASS END AS PER_ASS
  /* Factor variable for NCHARS modeling group */
    , CASE WHEN HD_CLASS IN (200,201,241) THEN 200
  WHEN HD_CLASS IN (299) THEN 299
  ELSE NULL END AS CONDO_CLASS_FACTOR
  /* Factor variable for mf modeling group */
    , CASE WHEN HD_CLASS IN (211, 212) THEN HD_CLASS
  ELSE NULL
  END AS MULTI_FAMILY_IND
  /* 1 acre is an arbitrary break here */
    , CASE WHEN HD_HD_SF > 43559 THEN 1
  ELSE 0
  END AS LARGE_LOT
  FROM
  HEAD AS H /* The HEAD file defines the universe of PINs that could have a sale associated with them */
    INNER JOIN /* Gives us each sale and the characteristics of the property IN THE YEAR it sold */
    (SELECT B.PIN, B.RECORDED_DATE as sale_date, B.SALE_PRICE as sale_price, B.DOC_NO, B.DEED_TYPE
     , TYPE_RESD, [USE], APTS, EXT_WALL, ROOF_CNST, ROOMS, BEDS, BSMT, BSMT_FIN, HEAT, OHEAT, AIR, FRPL, ATTIC_TYPE
     , ATTIC_FNSH, FBATH, HBATH, TP_PLAN, TP_DSGN, CNST_QLTY, RENOVATION, SITE, GAR1_SIZE, GAR1_CNST, GAR1_ATT, GAR1_AREA, GAR2_SIZE, GAR2_CNST, GAR2_ATT
     , GAR2_AREA, PORCH, OT_IMPR, BLDG_SF, REPAIR_CND, AGE, MULTI_CODE, VOLUME, NCU, MULTI_IND
     FROM
     IDORSALES AS B
     INNER JOIN
     (SELECT DISTINCT A.PIN, A.RECORDED_DATE, A.SALE_PRICE, MAX(A.DOC_NO) AS DOC_NO FROM /* Arbitrarily select a deed */
         IDORSALES AS A
       INNER JOIN
       (SELECT X.PIN, X.RECORDED_DATE, MAX(X.SALE_PRICE) AS SALE_PRICE FROM /* Largest sale price by PIN and date */
           IDORSALES AS X
         GROUP BY X.PIN, X.RECORDED_DATE
       ) AS SSS
       ON A.PIN=SSS.PIN AND A.RECORDED_DATE=SSS.RECORDED_DATE AND A.SALE_PRICE=SSS.SALE_PRICE
       GROUP BY A.PIN, A.RECORDED_DATE, A.SALE_PRICE
     ) AS WW
     ON WW.PIN=B.PIN AND WW.RECORDED_DATE=B.RECORDED_DATE AND WW.DOC_NO=B.DOC_NO
     LEFT JOIN
     CCAOSFCHARS AS CC /* Get characeristics for property in the year it sold */
       ON B.PIN=CC.PIN AND YEAR(B.RECORDED_DATE)=CC.TAX_YEAR
     WHERE (1=1)
     and B.DEED_TYPE NOT IN ('Q', 'E', 'B')
     AND MULT_IND = ''
     AND YEAR(B.RECORDED_DATE)>=2005
    ) AS INCEPTION
  ON HD_PIN=INCEPTION.PIN
  LEFT JOIN
  ( /* For addresses, census tracts, and xy */
      SELECT *,
    CASE WHEN PL_PIN IS NULL THEN Name ELSE PL_PIN END AS PIN_FILLED
    FROM PINLOCATIONS
    LEFT JOIN
    PROPLOCS
    ON LEFT(NAME, 10)=LEFT(PL_PIN, 10)
    WHERE primary_polygon IN (1)
    AND PIN999 NOT IN (1)
  ) AS X
  ON HD_PIN=PIN_FILLED
  LEFT JOIN /* Want BOR numbers for prior year */
    (SELECT PIN, LANDVAL, BLDGVAL FROM BAMASTER WHERE TAX_YEAR=2017) AS BOR
  ON HD_PIN=BOR.PIN
  LEFT JOIN/* Gives us the percentage of ownership for Condos */
    /* we take 399s as well as 299s here since some 299s were 399s in the previous year and we can't get info for them otherwise */
  (SELECT DISTINCT DT_PIN, (TAX_YEAR+1) AS TAX_YEAR, DT_CDU, DT_PER_ASS, DT_AGE AS NCHARS_AGE FROM DETAIL WHERE DT_CLASS in (299, 399)) AS DETAIL
  ON H.HD_PIN=DT_PIN AND DETAIL.TAX_YEAR=H.TAX_YEAR
LEFT JOIN /* Gives us a count of the number of units in condo buildings */
  (SELECT COUNT(HD_PIN) AS n_units, PIN10, TAX_YEAR FROM
    (SELECT DISTINCT HD_PIN, SUBSTRING(HD_PIN, 1, 10) AS PIN10, (TAX_YEAR+1) AS TAX_YEAR FROM HEAD
    WHERE HD_CLASS = 299) AS A
       GROUP BY PIN10, TAX_YEAR) AS AA
ON AA.PIN10=SUBSTRING(H.HD_PIN, 1, 10) AND H.TAX_YEAR=AA.TAX_YEAR
LEFT JOIN /* Allows us to proprate sale prices based on building size for multi properties */
  (SELECT CASE WHEN SUM(BLDG_SF) = 0 THEN LAG(SUM(BLDG_SF)) OVER (ORDER BY PIN, TAX_YEAR) ELSE SUM(BLDG_SF) END as total_bldg_sf, PIN, TAX_YEAR FROM CCAOSFCHARS
  WHERE MULTI_IND=1
  GROUP BY PIN, TAX_YEAR) AS PRO
ON PRO.PIN=H.HD_PIN AND PRO.TAX_YEAR=H.TAX_YEAR /* Note the slight difference here vs. the modeling data */
LEFT JOIN /* allows us to model condos within strata */
  /*(SELECT PIN10, TAX_YEAR, condo_strata_10, condo_strata_100 FROM CONDOSTRATA) AS STRATA*/
	(SELECT PIN10, TAX_YEAR, condo_strata FROM CONDOSTRATA) AS STRATA
ON LEFT(HD_PIN, 10) = STRATA.PIN10 AND H.TAX_YEAR = STRATA.TAX_YEAR
LEFT OUTER JOIN /*Excludes properties with a pending  inspection */
  (SELECT PIN, TAX_YEAR FROM PERMITTRACKING WHERE COMP_RECV=0) AS I
  ON I.PIN=H.HD_PIN AND I.TAX_YEAR=H.TAX_YEAR
WHERE (1=1)
  AND H.TAX_YEAR=2018
  AND primary_polygon IN (1)
  AND PIN999 NOT IN (1)
  AND HD_CLASS IN (211, 212, 200, 201, 241, 299, 202, 203, 204, 205, 206, 207, 208, 209, 210, 234, 278, 295)
  AND CONVERT(INT, SUBSTRING(CONVERT(CHARACTER, HD_TOWN),1,2)) IN (",modeling_townships,")
ORDER BY HD_PIN, HD_CLASS
"))