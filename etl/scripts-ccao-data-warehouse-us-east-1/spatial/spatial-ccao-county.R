library(arrow)
library(aws.s3)
library(dplyr)
library(geoarrow)
library(here)
library(sf)
library(stringr)
library(tidyr)
source("utils.R")

# This script cleans CCAO-specific shapefiles for townships and neighborhoods
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
input_bucket <- file.path(AWS_S3_RAW_BUCKET, "spatial", "ccao")
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, "spatial", "ccao")


##### COUNTY #####
remote_file_county_raw <- file.path(
  input_bucket, "county", "2019.geojson"
)
remote_file_county_warehouse <- file.path(
  output_bucket, "county", "2019.parquet"
)

if (!aws.s3::object_exists(remote_file_county_warehouse)) {
  tmp_file_county <- tempfile(fileext = ".geojson")
  aws.s3::save_object(remote_file_county_raw, file = tmp_file_county)

  st_read(tmp_file_county) %>%
    st_transform(4326) %>%
    rename_with(tolower) %>%
    mutate(
      geometry_3435 = st_transform(geometry, 3435),
    ) %>%
    select(geometry, geometry_3435) %>%
    geoparquet_to_s3(remote_file_county_warehouse)
}
