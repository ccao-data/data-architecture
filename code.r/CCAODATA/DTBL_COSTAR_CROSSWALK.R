library(odbc)
library(DBI)
library(magrittr)
library(tidyverse)
library(dplyr)

CCAODATA <- dbConnect(odbc(), .connection_string = Sys.getenv("DB_CONFIG_CCAODATA"))

# SQL PULLS ----

# fetch 300 class commercial apartments - NOT UNIQUE BY PIN DUE TO SALES -
query <- "SELECT
  HEAD.PIN AS [PIN], CASE WHEN LEN(DT_KEY_PIN) = 14 THEN CONVERT(VARCHAR, DT_KEY_PIN) ELSE NULL END AS [KEY_PIN],
  HEAD.TAX_YEAR, HD_CLASS AS CLASS, DT_AGE AS [AGE], (HD_ASS_LND + HD_ASS_BLD) * 10 AS [FMV],
  census_tract, township_code, TOWNS.township_name, triad_name, centroid_y AS lat, centroid_x AS long,
  sale_price, sale_date, DEED_NUMBER, COMMENTS

FROM AS_HEADT HEAD

  --- GRAB AGE AND KEY PIN
  LEFT JOIN AS_DETAILT DETAIL
  ON HEAD.PIN = DETAIL.PIN AND HEAD.TAX_YEAR = DETAIL.TAX_YEAR AND HEAD.HD_CLASS = DETAIL.DT_CLASS

  --- GRAB CENSUS TRACTS
  LEFT JOIN VW_PINGEO GEO
  ON HEAD.PIN = GEO.PIN

  --- GRAB TOWN NAMES AND TRIS
  INNER JOIN FTBL_TOWNCODES TOWNS
  ON LEFT(HD_TOWN, 2) = township_code

  --- GRAB VALIDATED SALES
  LEFT JOIN (

  SELECT PIN, sale_price, sale_date, DEED_NUMBER, COMMENTS

  FROM VW_CLEAN_IDORSALES SALES

  LEFT JOIN DTBL_VALIDATED_IC_SALES AS VALID
  ON SALES.DOC_NO = VALID.DEED_NUMBER

  WHERE VALID = 1

  ) SALES

  ON HEAD.PIN = SALES.PIN AND HEAD.TAX_YEAR = Year(SALES.sale_date)

--- LIMIT TO PINS THAT AREN'T BRAND NEW, COMMERCIAL APT BUILDINGS, LAST THREE YEARS
WHERE DT_AGE > 0
  AND HD_CLASS IN (313, 314, 315, 318, 391, 396)
  AND HEAD.TAX_YEAR >= 2018"

all_apartment_PINs <- dbGetQuery(CCAODATA, query) %>%
  
  # format PIN and rename columns
  mutate(PIN = pin_format_pretty(PIN, full_length = TRUE),
         KEY_PIN = pin_format_pretty(KEY_PIN, full_length = TRUE)) %>%
  rename_all(tolower) %>%
  
  # because there's no perfect way to merge head and detail,
  # we need to carefully make sure we don't have duplicate sales
  distinct() %>%
  dplyr::filter(!duplicated(.[c("deed_number", "sale_price")]) | is.na(deed_number))