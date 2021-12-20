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
input_bucket <- file.path(
  AWS_S3_RAW_BUCKET, "tax", "tax_bill_amounts"
)
output_bucket <- file.path(
  AWS_S3_WAREHOUSE_BUCKET, "tax", "tax_bill_amounts"
)

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
upload_taxbillamounts <- function(s3_bucket_uri, year, town_code) {

  # Only run if object doesn't already exist
  remote_file <- file.path(
    output_bucket,
    paste0("year=", year),
    paste0("town_code=", town_code),
    "part-0.parquet"
  )

  if (!aws.s3::object_exists(remote_file)) {
    message("Now fetching: ", year)

    taxbillamounts <- read_parquet(
      aws.s3::get_object(paste0(input_bucket, year, ".parquet")
      )
    ) %>%
      select(PIN, TAX_YEAR, TB_TOWN, TB_EAV:TB_EST_TAX_AMT) %>%
      rename_with(., ~ tolower(gsub("TB_", "", .x))) %>%
      rename(town_code = town) %>%
      select(-tax_year) %>%
      mutate(year = path["year"]) %>%
      group_by(year, town_code) %>%
      write_partitions_to_s3(s3_bucket_uri, is_spatial = FALSE)
  }
}

# Apply function to all files
pwalk(paths, function(...) {
  df <- tibble::tibble(...)
  upload_taxbillamounts(
    s3_bucket_uri = output_bucket,
    year = df$year,
    town_code = df$town_code
  )
})