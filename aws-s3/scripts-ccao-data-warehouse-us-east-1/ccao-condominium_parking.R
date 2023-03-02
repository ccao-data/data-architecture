library(arrow)
library(aws.s3)
library(dplyr)
library(purrr)
library(stringr)
library(tools)
source("utils.R")

# This script cleans and uploads condo parking space data for the warehouse
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(
  AWS_S3_WAREHOUSE_BUCKET,
  "ccao", "condominium"
)

##### QUESTIONABLE GARAGE UNITS #####

# Get S3 file addresses
files <- grep(
  ".parquet",
  file.path(
    AWS_S3_RAW_BUCKET,
    aws.s3::get_bucket_df(
      AWS_S3_RAW_BUCKET,
      prefix = "ccao/condominium/pin_questionable_garage_units/")$Key
  ),
  value = TRUE
)

# Function to read and amend questionable parking spaces
read_questionable <- function(x) {

  read_parquet(x) %>%
    filter(!str_detect(X3, "storage") | is.na(X3)) %>%
    mutate(year = tools::file_path_sans_ext(basename(x))) %>%
    select("pin" = 1, "year") %>%
    filter(str_detect(pin, "^[:digit:]+$")) %>%
    mutate(pin = str_pad(pin, 14, side = 'left', pad = '0'))

}

# Load raw files, cleanup, then write to warehouse S3
map(files, read_questionable) %>%
  bind_rows() %>%
  group_by(year) %>%
  arrow::write_dataset(
    path = file.path(output_bucket, "pin_questionable_garage_units"),
    format = "parquet",
    hive_style = TRUE,
    compression = "snappy"
  )

##### 399 GARAGE UNITS #####

# Get S3 file addresses
files <- grep(
  ".parquet",
  file.path(
    AWS_S3_RAW_BUCKET,
    aws.s3::get_bucket_df(
      AWS_S3_RAW_BUCKET,
      prefix = "ccao/condominium/pin_399_garage_units/")$Key
  ),
  value = TRUE
)

# Function to read and amend questionable parking spaces
read_399s <- function(x) {

  read_parquet(x) %>%
    mutate(across(.cols = everything(), ~ as.character(.x))) %>%
    rename_with(tolower)

}

# Load raw files, cleanup, then write to warehouse S3
map(files, read_399s) %>%
  bind_rows() %>%
  group_by(taxyr) %>%
  arrow::write_dataset(
    path = file.path(output_bucket, "pin_399_garage_units"),
    format = "parquet",
    hive_style = TRUE,
    compression = "snappy"
  )
