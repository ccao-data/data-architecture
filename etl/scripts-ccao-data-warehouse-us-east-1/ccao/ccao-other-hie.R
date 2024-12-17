library(arrow)
library(aws.s3)
library(DBI)
library(dplyr)
library(lubridate)
library(odbc)
library(purrr)
library(stringr)
library(tidyr)
source("utils.R")

# This script retrieves and cleans home improvement exemption data stored in
# the CCAO's legacy AS/400 system
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, "ccao", "other", "hie")

# Connect to legacy CCAO SQL server
CCAODATA <- dbConnect(
  odbc(),
  .connection_string = Sys.getenv("DB_CONFIG_CCAODATA")
)

# Grab all legacy HIE data from the ADDCHARS table
hie <- DBI::dbGetQuery(
  CCAODATA,
  "
  SELECT
    QU_PIN,
    QU_TOWN,
    QU_MLT_CD,
    QU_HOME_IMPROVEMENT,
    QU_USE,
    QU_EXTERIOR_WALL,
    QU_ROOF,
    QU_BASEMENT_TYPE,
    QU_BASEMENT_FINISH,
    QU_HEAT,
    QU_AIR,
    QU_ATTIC_TYPE,
    QU_ATTIC_FINISH,
    QU_TYPE_PLAN,
    QU_TYPE_DESIGN,
    QU_CONSTRUCT_QUALITY,
    QU_PORCH,
    QU_GARAGE_SIZE,
    QU_GARAGE_CONST,
    QU_GARAGE_ATTACHED,
    QU_GARAGE_AREA,
    QU_NUM_APTS,
    QU_SQFT_BLD,
    QU_LND_SQFT,
    QU_CLASS,
    QU_ROOMS,
    QU_BEDS,
    QU_FULL_BATH,
    QU_HALF_BATH,
    QU_FIRE_PLACE,
    QU_NO__COM_UNIT,
    QU_TYPE_OF_RES,
    QU_UPLOAD_DATE,
    TAX_YEAR
  FROM ADDCHARS
  WHERE QU_HOME_IMPROVEMENT = 1
  "
)

# Clean up raw ADDCHARS data
hie_clean <- hie %>%
  mutate(
    QU_CLASS = as.numeric(stringr::str_sub(QU_CLASS, 1, 3)),
    QU_PIN = str_pad(QU_PIN, 14, "left", "0"),
    hie_last_year_active = map_chr(
      ccao::chars_288_active(TAX_YEAR, as.character(QU_TOWN)),
      ~ tail(.x, n = 1)
    ),
    QU_NO__COM_UNIT = as.numeric(QU_NO__COM_UNIT),
    QU_NO__COM_UNIT = replace_na(QU_NO__COM_UNIT, 0),
    across(
      c(QU_TOWN:QU_NUM_APTS, QU_CLASS, QU_TYPE_OF_RES, TAX_YEAR),
      as.character
    ),
    across(where(is.character), na_if, " "),
    QU_CLASS = na_if(QU_CLASS, "0"),
    # Convert upload date to date format and if missing, set as the earliest
    # date for the year
    QU_UPLOAD_DATE = lubridate::ymd(QU_UPLOAD_DATE),
    QU_UPLOAD_DATE = lubridate::as_date(ifelse(
      is.na(QU_UPLOAD_DATE),
      lubridate::make_date(as.numeric(TAX_YEAR), 1, 1),
      QU_UPLOAD_DATE
    )),
  ) %>%
  rename_with(tolower) %>%
  rename(pin = qu_pin, year = tax_year, qu_no_com_unit = qu_no__com_unit)

# Save HIE data to warehouse, partitioned by year
hie_clean %>%
  mutate(loaded_at = as.character(Sys.time())) %>%
  group_by(year) %>%
  arrow::write_dataset(
    path = output_bucket,
    format = "parquet",
    hive_style = TRUE,
    compression = "snappy"
  )
