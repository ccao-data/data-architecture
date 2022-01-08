library(arrow)
library(aws.s3)
library(DBI)
library(dplyr)
library(odbc)
library(purrr)
source("utils.R")

# This script retrieves a raw version of the CCAODATA SQL
# table TAXBILLAMOUNTS
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
output_bucket <- file.path(AWS_S3_RAW_BUCKET, "tax", "bill_amount")

# Connect to CCAODATA SQL server
CCAODATA <- dbConnect(
  odbc::odbc(),
  .connection_string = Sys.getenv("DB_CONFIG_CCAODATA")
)

# Gather the data
billamount <- dbGetQuery(
  CCAODATA,
  paste0("SELECT * FROM TAXBILLAMOUNTS")
) %>%
  split(.$TAX_YEAR)

# Function to write each year of TAXBILLAMOUNTS to a parquet file on S3
upload_billamount <- function(data, name) {
  remote_file_path <- file.path(output_bucket, paste0(name, ".parquet"))

  if (!aws.s3::object_exists(remote_file_path)) {
    write_parquet(data, remote_file_path)
    message(remote_file_path, " complete.")
  } else {
    message(remote_file_path, " skipped.")
  }
}

# Save all of table to S3
mapply(
  upload_billamount,
  data = billamount,
  name = names(billamount)
)

# Cleanup
dbDisconnect(CCAODATA)