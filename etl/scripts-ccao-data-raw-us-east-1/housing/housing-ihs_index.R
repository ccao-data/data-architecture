library(arrow)
library(dplyr)
library(janitor)
library(openxlsx)
library(rvest)
library(tibble)

# This script retrieves raw DePaul IHS data for the data lake
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

# Grab the data, clean it just a bit, and write if it doesn't already exist.
# Ignore columns before 5 and after 41: columns 1-4 are aggregated values for
# the city, county, and suburbs, while columns 42+ are racial breakouts.

# It's worth noting that the file read into R using the link scraped from the
# site's html is not the same as the file downloaded manually from the site. The
# latter contains PUMA names and is formatted with frozen panes which I suspect
# is causing the difference. I made sure this isn't just an artifact of
# openxlsx::read.xlsx() by downloading the file from the scraped html and
# reading it in with readxl::read_excel() (which can handle frozen panes),
# which produced the same result as reading in the file from the link. Finally,
# the link scraped from the html is NOT the same as the link the download button
# on the page points to, though the underlying data are the same: the html
# points to a self-hosted file while the download button points to AWS S3.
data.frame(t(
  read.xlsx(most_recent_ihs_data_url, sheet = 2, cols = 5:41)
)) %>%
  # Names and columns are kind of a mess after the transpose,
  # shift up first row, shift over column names
  row_to_names(1) %>%
  rownames_to_column("puma") %>%
  write_parquet(remote_file)
