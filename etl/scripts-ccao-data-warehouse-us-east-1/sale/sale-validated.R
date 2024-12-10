library(arrow)
library(aws.s3)
library(dplyr)
library(glue)
library(openxlsx)
library(purrr)
library(stringr)
library(tidyr)
source("utils.R")

# This script cleans and combines raw validated data for the warehouse
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, "sale", "validated")

# Location of raw data
raw_files <- grep(
  ".xlsx",
  file.path(
    AWS_S3_RAW_BUCKET,
    aws.s3::get_bucket_df(AWS_S3_RAW_BUCKET, prefix = "sale/validated/")$Key
  ),
  value = TRUE
)

clean_validated <- function(raw_file) {
  tmp_file <- tempfile(fileext = ".xlsx")
  aws.s3::save_object(raw_file, file = tmp_file)

  temp <- read.xlsx(tmp_file, sheet = 1) %>%
    rename_with(~ tolower(gsub("\\?|\\,|\\'", "", gsub("\\.", "_", .x)))) %>%
    rename(
      deed_number = `deed_number`,
      valid = `arms_length_transaction`,
      comments = `notes`,
      year_of_sale = sale_year
    ) %>%
    filter(!is.na(valid)) %>%
    mutate(
      pin = substr(gsub("-", "", pin), 1, 10),
      deed_number = str_pad(deed_number, 11, side = "left", pad = "0"),
      valid = case_when(
        valid %in% c("yes", "Y", "Yes") ~ TRUE,
        valid %in% c("no", "N", "No", "Not Sure") ~ FALSE,
        is.na(valid) ~ FALSE,
        TRUE ~ FALSE
      )
    ) %>%
    select(
      pin, deed_number, reason_for_flag, condition,
      interior_pictures, renovated, demolished_new_house,
      characteristics_match_up, if_not_what_is_different, valid, comments, year_of_sale
    ) %>%
    distinct() %>%
    group_by(year_of_sale) %>%
    write_partitions_to_s3(
      output_bucket,
      is_spatial = FALSE,
      overwrite = TRUE
    )
}

walk(raw_files, clean_validated)
