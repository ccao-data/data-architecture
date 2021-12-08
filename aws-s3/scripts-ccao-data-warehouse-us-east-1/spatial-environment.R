library(arrow)
library(aws.s3)
library(dplyr)
library(here)
library(purrr)
library(sf)
library(sfarrow)
library(stringr)
library(tidyr)

# This script cleans environmental data such as floodplains and coastal
# boundaries and uploads them to S3 as parquet files
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")

##### LAKE MICHICAN COASTLINE #####
remote_file_coastline_raw <- file.path(
  AWS_S3_RAW_BUCKET, "spatial", "environment", "coastline",
  paste0("2021.geojson")
)
remote_file_coastline_warehouse <- file.path(
  AWS_S3_WAREHOUSE_BUCKET, "spatial", "environment", "coastline",
  paste0("2021.geojson")
)
tmp_file_coastline <- tempfile(fileext = ".geojson")

if (!aws.s3::object_exists(remote_file_coastline_warehouse)) {

}