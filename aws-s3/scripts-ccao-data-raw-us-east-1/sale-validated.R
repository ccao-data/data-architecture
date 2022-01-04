library(aws.s3)
library(DBI)
library(dplyr)
source("utils.R")

# This script retrieves validated sales information provvided by valuations and stored on Gitlab
# THIS SOURCE WILL NEED TO BE UPDATED
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
output_bucket <- file.path(AWS_S3_RAW_BUCKET, "sale", "validated")

# Get S3 file address
s3_uri <- file.path(output_bucket, paste0("2021-01-04", ".xlsx"))

# Retrieve data and write to S3
tmp_file <- tempfile(fileext = ".xlsx")
tmp_dir <- tempdir()

download.file(
  'https://gitlab.com/ccao-data-science---modeling/processes/etl_validated_sales/-/raw/master/residential/data/2021-01-04_validated_res_sales.xlsx',
  tmp_file,
  mode = "wb"
)

save_local_to_s3(s3_uri, tmp_file, overwrite = TRUE)

# Clean up
file.remove(tmp_file)
