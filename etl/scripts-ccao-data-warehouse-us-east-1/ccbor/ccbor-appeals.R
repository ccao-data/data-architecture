library(arrow)
library(dplyr)
library(purrr)
library(stringr)
library(tidyr)
source("utils.R")

# This script retrieves and BOR appeals data and formats it for use in Athena
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
input_bucket <- file.path(AWS_S3_RAW_BUCKET, "ccabor", "appeals")
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, "ccbor", "appeals")

# Grab the file paths for the raw data on S3
raw_paths <- aws.s3::get_bucket_df(
  AWS_S3_RAW_BUCKET,
  prefix = "ccbor/appeals/"
) %>%
  filter(str_detect(Key, "parquet")) %>%
  pull(Key)

# Load the raw appeals data, rename and clean up columns, then write to S3
# partitioned by year
map(raw_paths, \(x) {
  read_parquet(file.path(AWS_S3_RAW_BUCKET, x))
} ) %>%
  bind_rows() %>%
  select(-c(pin10:centroid_geom.coordinates)) %>%
  rename(taxyr = tax_year) %>%
  group_by(taxyr) %>%
  write_partitions_to_s3(
    output_bucket,
    is_spatial = FALSE,
    overwrite = TRUE
  )
