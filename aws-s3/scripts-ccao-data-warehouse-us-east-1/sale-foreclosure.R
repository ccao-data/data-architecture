library(arrow)
library(aws.s3)
library(dplyr)
library(tidyr)
library(readr)
library(tools)
library(glue)
library(data.table)
library(lubridate)

# This script cleans and combines raw foreclosure data for the warehouse
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")

# Destination for upload
dest_file <- file.path(
  AWS_S3_WAREHOUSE_BUCKET,
  max(aws.s3::get_bucket_df(AWS_S3_RAW_BUCKET, prefix = 'sale/foreclosure/')$Key)
)

# Get S3 file addresses
files <- grep(
  ".parquet",
  file.path(
    AWS_S3_RAW_BUCKET,
    aws.s3::get_bucket_df(AWS_S3_RAW_BUCKET, prefix = 'sale/foreclosure/')$Key
    ),
  value = TRUE
  )

lapply(files, read_parquet) %>%
  rbindlist() %>%
  rename_with(~ tolower(gsub(" ", "_" ,.x))) %>%
  rename(PIN = property_identification_number) %>%
  mutate(PIN = gsub("[^0-9.-]", "", PIN)) %>%
  select(PIN, case_number, document_number,
         date_of_sale, sale_results, sold_amount,
         original_sale_date, company_name, real_estate_auction,
         bankruptcy_filed) %>%
  separate(bankruptcy_filed, sep = " - Chapter ", into = c(NA, "bankruptcy_chapter")) %>%
  mutate(year = lubridate::year(date_of_sale)) %>%
  group_by(year) %>%
  group_walk(~ {
    year <- replace_na(.y$year, "__HIVE_DEFAULT_PARTITION__")
    remote_path <- file.path(
      AWS_S3_WAREHOUSE_BUCKET, "sale", "foreclosure",
      paste0("year=", year),
      "part-0.parquet"
    )
    if (!object_exists(remote_path)) {
      print(paste("Now uploading:", year, "data for year:", year))
      tmp_file <- tempfile(fileext = ".parquet")
      write_parquet(.x, tmp_file, compression = "snappy")
      aws.s3::put_object(tmp_file, remote_path)
    }
  })

# Cleanup
rm(list = ls())
