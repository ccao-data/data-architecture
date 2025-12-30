library(arrow)
library(aws.s3)
library(dplyr)
library(glue)
library(purrr)
library(readr)
library(tools)
source("utils.R")

# This script retrieves raw foreclosure sales from the CCAO's O Drive. Data
# comes from https://beta-www.public-record.com/. Instructions for accessing the
# website and downloading the data can be found here:
# O:\CCAODATA\documentation\How to Download Foreclosure Data.docx
# The script itself does NOT update the data; it only gathers what has been
# downloaded and uploads it to S3.
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
output_bucket <- file.path(AWS_S3_RAW_BUCKET, "sale", "foreclosure")

# Get S3 file address
files <- list.files("O:/CCAODATA/data/foreclosures", recursive = TRUE)

# Function to retrieve data and write to S3
read_write <- function(x) {
  output_dest <- file.path(output_bucket, glue(parse_number(x), ".parquet"))

  if (!object_exists(output_dest)) {
    print(output_dest)

    readr::read_delim(
      glue("O:/CCAODATA/data/foreclosures/", x),
      delim = ","
    ) %>%
      write_parquet(output_dest)
  }
}

# Apply function to foreclosure data
walk(files, read_write)
