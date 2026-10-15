library(arrow)
library(dplyr)
library(janitor)
library(openxlsx)
library(rvest)
library(stringr)
library(tibble)

# This script retrieves raw DePaul IHS data for the data lake. It assumes two
# things about the imported .xlsx:
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
  read.xlsx(most_recent_ihs_data_url, sheet = 2, cols = 5:41)
)) %>%
  # Names and columns are kind of a mess after the transpose,
  # shift up first row, shift over column names
  row_to_names(1) %>%
  rownames_to_column("puma") %>%
  mutate(puma = str_replace(puma, "HP1U_", "")) %>%
  write_parquet(remote_file)
