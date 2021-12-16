library(arrow)
library(aws.s3)
library(DBI)
library(dplyr)
library(odbc)
library(stringr)

# This script retrieves raw CCRD sales from the Data Department's SQL server
# THIS SOURCE WILL NEED TO BE UPDATED
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")

# Connect to CCAODATA SQL server
CCAODATA <- odbc::dbConnect(
  odbc::odbc(),
  .connection_string = Sys.getenv("DB_CONFIG_CCAODATA")
)

# Get S3 file address
remote_file <- file.path(
  AWS_S3_RAW_BUCKET, "sale", "ccrd",
  paste0("ccrd", ".parquet")
)

# Retrieve data and write to S3
DBI::dbGetQuery(
  CCAODATA,
  "SELECT
     DOC_NO,
     SALE_PRICE,
     RECORDED_DATE,
     EXECUTED_DATE,
     SELLER_NAME,
     BUYER_NAME,
     PIN
  FROM DTBL_CCRDSALES
  WHERE YEAR(EXECUTED_DATE) >= 2013
  AND SALE_PRICE IS NOT NULL"
) %>%
  write_parquet(remote_file)

# Cleanup
dbDisconnect(CCAODATA)
rm(list = ls())
