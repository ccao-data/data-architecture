library(arrow)
library(aws.s3)
library(dplyr)
library(glue)
library(purrr)
library(readr)
library(tools)
library(rvest)
library(stringr)
source("utils.R")

# This script retrieves raw mydec data from Illinois Department of Revenue
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
output_bucket <- file.path(AWS_S3_RAW_BUCKET, "sale", "mydec")

# Mydec file addresses
files <- xml2::read_html(
  "https://tax.illinois.gov/localgovernments/property/mydecdatafiles.html"
) %>%
  html_nodes("a") %>%
  html_attr("href") %>%
  str_subset("ptax203")

# Function to scrape IDOR data and write to S3
down_up <- function(x) {
  year <- str_extract(x, pattern = "[0-9]{4}")

  if (
    !aws.s3::object_exists(file.path(output_bucket, glue("{year}.parquet")))
  ) {
    print(glue("Uploading data for: {year}"))

    tmp1 <- tempfile(fileext = ".zip")
    tmp2 <- tempfile()

    # Grab zipped file
    # https://tax.illinois.gov/localgovernments/property/mydecdatafiles.html
    download.file(glue("https://tax.illinois.gov{x}"), destfile = tmp1)

    # Unzip
    unzip(tmp1, exdir = tmp2, overwrite = TRUE)

    # upload to S3
    readr::read_delim(list.files(tmp2, full.names = TRUE), delim = "\t") %>%
      write_parquet(file.path(output_bucket, glue("{year}.parquet")))
  }
}

# Apply function to foreclosure data
walk(files, down_up)
