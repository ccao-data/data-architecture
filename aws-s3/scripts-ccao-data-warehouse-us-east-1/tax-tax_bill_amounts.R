library(arrow)
library(aws.s3)
library(ccao)
library(dplyr)
library(purrr)
library(stringr)
source("utils.R")

# This script cleans saved data from the county's mainframe pertaining to tax bills
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")

# Get a list of all TAXBILLAMOUNTS objects and their associated townships in S3
paths <- aws.s3::get_bucket_df(
  bucket = AWS_S3_RAW_BUCKET,
  prefix = "tax/tax_bill_amounts"
) %>%
  filter(Size > 0) %>%
  pull(Key) %>%
  str_match("[0-9]{4}") %>%
  expand.grid(
    year = ., town_code = ccao::town_dict$township_code,
    stringsAsFactors = FALSE
  )

# Function to load file from S3, clean and filter column names, and partition data
upload_taxbillamounts <- function(path) {

  # Only run if object doesn't already exist
  remote_file <- file.path(
    AWS_S3_WAREHOUSE_BUCKET, "tax", "tax_bill_amounts",
    paste0("year=", path["year"]),
    paste0("town_code=", path["town_code"]),
    "part-0.parquet"
  )

  if (!aws.s3::object_exists(remote_file)) {
    message("Now fetching: ", path["year"])

    taxbillamounts <- read_parquet(
      aws.s3::get_object(
        paste0(
          "s3://ccao-data-raw-us-east-1/tax/tax_bill_amounts/",
          path["year"], ".parquet"
        )
      )
    ) %>%
      select(PIN, TAX_YEAR, TB_TOWN, TB_EAV:TB_EST_TAX_AMT) %>%
      rename_with(., ~ tolower(gsub("TB_", "", .x))) %>%
      rename(town_code = town) %>%
      select(-tax_year) %>%
      mutate(year = path["year"]) %>%
      group_by(year, town_code) %>%
      write_partitions_to_s3(output_bucket, is_spatial = FALSE)
  }
}

# Apply function to all files
apply(paths, 1, upload_taxbillamounts)