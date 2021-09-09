library(here)
library(dplyr)
library(arrow)
library(odbc)
library(DBI)


# this script retrieves raw CCRD sales from the data department's SQL server
# THIS SOURCE WILL NEED TO BE UPDATED

# connect to CCAODATA SQL server
CCAO <- odbc::dbConnect(odbc::odbc(),
                        .connection_string = Sys.getenv("DB_CONFIG_CCAODATA"))

# retrieve data
DBI::dbGetQuery(CCAO, paste0(

  "SELECT DOC_NO, SALE_PRICE, RECORDED_DATE, EXECUTED_DATE, SELLER_NAME, BUYER_NAME, PIN FROM DTBL_CCRDSALES
  WHERE YEAR(EXECUTED_DATE) >= 2013 AND SALE_PRICE IS NOT NULL"

  )) %>%

  # output
  arrow::write_parquet(here("s3-bucket/sale/ccrd/ccrd.parquet"))

# clean
rm(list = ls())
