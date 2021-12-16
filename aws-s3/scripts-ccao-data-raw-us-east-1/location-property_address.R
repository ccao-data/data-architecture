library(arrow)
library(aws.s3)
library(DBI)
library(dplyr)
library(odbc)
library(stringr)

# This script retrieves raw addresses from the PROPLOCS table in CCAODATA
# THIS SOURCE WILL NEED TO BE UPDATED
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")

# Connect to CCAODATA SQL server
CCAODATA <- odbc::dbConnect(
  odbc::odbc(),
  .connection_string = Sys.getenv("DB_CONFIG_CCAODATA")
)

# Get S3 file address
remote_file <- file.path(
  AWS_S3_RAW_BUCKET, "location", "property_address",
  paste0("property_address", ".parquet")
)

# Retrieve data and write to S3 if not exists
if (!aws.s3::object_exists(remote_file)) {
  DBI::dbGetQuery(CCAODATA, "SELECT * FROM [PROPLOCS]") %>%
    dplyr::mutate(
      # Construct address
      address = stringr::str_squish(
        paste(
          PL_HOUSE_NO,
          PL_DIR, PL_STR_NAME,
          PL_STR_SUFFIX,
          sep = " "
        )
      ),
      # Format zip code
      zip = dplyr::case_when(
        nchar(.$PL_ZIPCODE) == 9 ~ paste(
          substr(.$PL_ZIPCODE, 1, 5),
          substr(.$PL_ZIPCODE, 6, 9),
          sep = "-"
        ),
        TRUE ~ ""
      ),
      # Format date address was updated
      updated = as.Date(as.character(PL_UPDT_DATE), "%Y%m%d")
    ) %>%
    dplyr::rename(city = PL_CITY_NAME, apt = PL_APT_NO, PIN = PL_PIN) %>%
    dplyr::select(PIN, address, city, zip, apt, updated) %>%
    arrow::write_parquet(remote_file)
}

# Cleanup
dbDisconnect(CCAODATA)
rm(list = ls())
