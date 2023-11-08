library(arrow)
library(aws.s3)
library(dplyr)
library(geoarrow)
library(glue)
library(noctua)
library(osmdata)
library(purrr)
library(sf)
source("utils.R")

# This script ingests data from the AWS raw bucket, simplifies the number of
# nodes for computational speed-up, and writes the resulting output to the
# AWS warehouse bucket

# Instantiate S3 bucket names
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")

# Get current year
current_year <- strftime(Sys.Date(), "%Y")

# Ingest path
ingest_file <- file.path(
  AWS_S3_RAW_BUCKET,"spatial",
  "environment", "secondary_road",
  paste0("year=", current_year),
  paste0("secondary_road-", current_year, ".parquet"))

# Output path
remote_file <- file.path(
  AWS_S3_WAREHOUSE_BUCKET, "spatial",
  "environment", "secondary_road",
  paste0("year=", current_year),
  paste0("secondary_road-", current_year, ".parquet")
)

if (!aws.s3::object_exists(remote_file)) {

  simplified_data <- read_geoparquet_sf(ingest_file) %>%
    mutate(geometry_3435 = st_simplify(geometry_3435, dTolerance = 10))

  geoarrow::write_geoparquet(simplified_data, remote_file)
}