library(aws.s3)
library(arrow)
library(dplyr)
library(purrr)
library(sf)
library(tigris)
source("utils.R")

# This script cleans geometry files and moves them to a specific folder for use
# in Tableau and other visualization software
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, "export")

##### 2020 Census tracts #####
remote_file_tract_2020 <- file.path(
  output_bucket, "geojson",
  "census-tract-2020.geojson"
)
if (!aws.s3::object_exists(remote_file_tract_2020)) {
  tracts_2020 <- tigris::tracts(
    state = "17",
    county = "031",
    cb = TRUE,
    year = 2020
  ) %>%
    st_transform(4326)

  # Write geojson to S3
  tmp_file_geojson <- tempfile(fileext = ".geojson")
  st_write(tracts_2020, tmp_file_geojson)
  save_local_to_s3(remote_file_tract_2020, tmp_file_geojson)
}