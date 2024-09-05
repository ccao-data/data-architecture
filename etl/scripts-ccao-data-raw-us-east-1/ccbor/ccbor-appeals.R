library(arrow)
library(glue)
library(dplyr)
library(jsonlite)
library(purrr)
library(RSocrata)

# This script retrieves Board of Review appeals data from the County's Open
# Data Portal
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
output_bucket <- file.path(AWS_S3_RAW_BUCKET, "ccbor", "appeals")

# Determine years for which data is available
years <- read_json(
  glue(
    "https://datacatalog.cookcountyil.gov/resource/7pny-nedm.json?",
    "$select=distinct(tax_year)"
  ),
  simplifyVector = TRUE
) %>%
  pull() %>%
  as.numeric()

# Gather BOR appeals data by year
walk(years, \(x) {
  remote_path <- file.path(output_bucket, paste0(x, ".parquet"))

  # Only gathers data if it doesn't already exist or is one of the two most
  # recent years of available data
  if (!aws.s3::object_exists(remote_path) | x >= (max(years) - 1)) {
    print(paste0("Fetching BOR Appeals Data for ", x))
    read.socrata(
      glue(
        "https://datacatalog.cookcountyil.gov/resource/7pny-nedm.json?",
        "$where=tax_year={x}"
      ),
      app_token = Sys.getenv("SOCRATA_APP_TOKEN"),
      email = Sys.getenv("SOCRATA_EMAIL"),
      password = Sys.getenv("SOCRATA_PASSWORD")
    ) %>%
      write_parquet(remote_path)
  }
}, .progress = TRUE)
