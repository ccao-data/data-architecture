library(arrow)
library(aws.s3)
library(dplyr)
library(purrr)
library(readr)
library(stringr)
source("utils.R")

# This script retrieves CCAO-specific dictionaries from the raw data bucket
# and transforms them into Athena tables
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
input_bucket <- file.path(AWS_S3_RAW_BUCKET, "ccao", "dictionary")
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, "ccao", "dictionary")

##### CLASS DICTIONARY #####
# Location of remote files
remote_file_class_dict <- file.path(input_bucket, "class_dict.csv")

# Temp file to download workbook
tmp_file_class_dict <- tempfile(fileext = ".csv")

# Grab the workbook from the raw S3 bucket
aws.s3::save_object(
  object = remote_file_class_dict,
  file = tmp_file_class_dict
)

# Read as CSV, clean up, write to parquet
readr::read_csv(
  file = tmp_file_class_dict,
  col_types = readr::cols(class_code = readr::col_character())
) %>%
  write_parquet(file.path(output_bucket, "class_dict", "part-0.parquet"))

