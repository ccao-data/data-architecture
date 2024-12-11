library(arrow)
library(aws.s3)
library(dplyr)
library(janitor)
library(openxlsx)
library(rvest)
source("utils.R")

# This script retrieves raw DePaul IHS data for the data lake
# It assumes a couple things about the imported .xlsx:
# - Three unnamed columns renamed "X1", "X2", and "X3" by R and
# - The value of the first row/column being "YEARQ"
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
output_bucket <- file.path(AWS_S3_RAW_BUCKET, "housing", "ihs_index")

# Scrape main page for .xlsx, which should be most recent release
most_recent_ihs_data_url <- rvest::read_html(
  "https://price-index.housingstudies.org/"
) %>%
  rvest::html_nodes(xpath = ".//a[contains(@href, '.xlsx')]") %>%
  rvest::html_attr("href") %>%
  sprintf("https://price-index.housingstudies.org%s", .)

# Get S3 file address
remote_file <- file.path(output_bucket, paste0("ihs_price_index_data.parquet"))

# Grab the data, clean it just a bit, and write if it doesn't already exist
data.frame(t(
  openxlsx::read.xlsx(most_recent_ihs_data_url, sheet = 2) %>%
    dplyr::select(-c("X2", "X3", "X4"))
)) %>%
  # Names and columns are kind of a mess after the transpose,
  # shift up first row, shift over column names
  janitor::row_to_names(1) %>%
  dplyr::mutate(puma = rownames(.)) %>%
  dplyr::relocate(puma, .before = "YEARQ") %>%
  dplyr::rename(name = "YEARQ") %>%
  arrow::write_parquet(remote_file)
