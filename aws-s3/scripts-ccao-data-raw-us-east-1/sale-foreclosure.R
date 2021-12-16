library(arrow)
library(aws.s3)
library(dplyr)
library(readr)
library(tools)
library(glue)

# This script retrieves raw foreclosure sales from the CCAO's O Drive
# THIS SOURCE WILL NEED TO BE UPDATED
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")

# Get S3 file address
files <- list.files("O:/CCAODATA/data/foreclosures", recursive = TRUE)

# Function to retrieve data and write to S3
read_write <- function(x) {
  readr::read_delim(
    glue("O:/CCAODATA/data/foreclosures/", x),
    delim = ","
  ) %>%
    write_parquet(
      file.path(
        AWS_S3_RAW_BUCKET, "sale", "foreclosure", glue(parse_number(x), ".parquet")
      )
    )
}

# Apply function to foreclosure data
lapply(files, read_write)

# Cleanup
rm(list = ls())
