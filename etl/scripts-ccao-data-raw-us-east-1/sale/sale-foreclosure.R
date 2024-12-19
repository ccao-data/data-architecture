library(arrow)
library(aws.s3)
library(dplyr)
library(glue)
library(purrr)
library(readr)
library(tools)
source("utils.R")

# This script retrieves raw foreclosure sales from the CCAO's O Drive
# THIS SOURCE WILL NEED TO BE UPDATED
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
output_bucket <- file.path(AWS_S3_RAW_BUCKET, "sale", "foreclosure")

# Get S3 file address
files <- list.files("O:/CCAODATA/data/foreclosures", recursive = TRUE)

# Function to retrieve data and write to S3
read_write <- function(files, x) {
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
