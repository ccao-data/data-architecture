library(here)
library(dplyr)
library(arrow)
library(odbc)
library(DBI)
library(stringr)

# this script retrieves raw addresses from the PROPLOCS table in CCAODATA

# connect to CCAODATA SQL server
CCAO <- dbConnect(odbc::odbc(),
                  .connection_string = Sys.getenv("DB_CONFIG_CCAODATA"))

# retrieve data
pull <- dbGetQuery(CCAO, paste0("SELECT * FROM [PROPLOCS]"))

proplocs <- pull %>%

  mutate(

    # construct address
    address = stringr::str_squish(
      paste(
        PL_HOUSE_NO,
        PL_DIR, PL_STR_NAME,
        PL_STR_SUFFIX,
        sep = " "
        )
      ),

    # format zip code
    zip = case_when(
      nchar(.$PL_ZIPCODE) == 9 ~ paste(
        substr(.$PL_ZIPCODE, 1, 5),
        substr(.$PL_ZIPCODE, 6, 9),
        sep = "-"
        ),
      TRUE ~ ""),

    # format date address was updated
    updated = as.Date(as.character(PL_UPDT_DATE), "%Y%m%d")
  ) %>%

  rename(
    city = PL_CITY_NAME,
    apt = PL_APT_NO,
    PIN = PL_PIN
  ) %>%

  select(PIN, address, city, zip, apt, updated) %>%

  # output
  arrow::write_parquet(here("s3-bucket/stable/location/property_address/property_address.parquet"))

# clean
rm(list = ls())
