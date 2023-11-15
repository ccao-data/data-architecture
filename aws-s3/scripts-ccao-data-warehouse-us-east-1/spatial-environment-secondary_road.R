library(arrow)
library(aws.s3)
library(dplyr)
library(geoarrow)
library(osmdata)
library(sf)
source("utils.R")

# This script ingests data from the AWS raw bucket, simplifies the number of
# nodes for computational speed-up, and writes the resulting output to the
# AWS warehouse bucket

# Instantiate S3 bucket names
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")

# Get current year as an integer
current_year <- as.integer(strftime(Sys.Date(), "%Y"))

# Create a sequence of years from 2014 to the current year
years <- 2014:current_year

# Iterate over the years
for (year in years) {
  # Update the ingest and output paths for each year
  ingest_file <- file.path(
    AWS_S3_RAW_BUCKET, "spatial",
    "environment", "secondary_road",
    paste0("year=", year),
    paste0("secondary_road-", year, ".parquet"))

  remote_file <- file.path(
    AWS_S3_WAREHOUSE_BUCKET, "spatial",
    "environment", "secondary_road",
    paste0("year=", year),
    paste0("secondary_road-", year, ".parquet")
  )

  if (!aws.s3::object_exists(remote_file)) {
    simplified_data <- read_geoparquet_sf(ingest_file) %>%
      mutate(geometry_3435 = st_simplify(geometry_3435, dTolerance = 10))

    geoarrow::write_geoparquet(simplified_data, remote_file)
  }
}
