library(arrow)
library(aws.s3)
library(dplyr)
library(purrr)
library(tools)
source("utils.R")

# This script cleans and combines raw foreclosure data for the warehouse
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(
  AWS_S3_WAREHOUSE_BUCKET,
  "ccao", "condominium"
)

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
    mutate(year = tools::file_path_sans_ext(basename(x)))

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
