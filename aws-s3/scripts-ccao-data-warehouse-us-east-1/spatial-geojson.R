library(aws.s3)
library(arrow)
library(dplyr)
library(purrr)
library(sf)
library(tigris)

# This script cleans geometry files and moves them to a specific folder for use
# in Tableau and other visualization software
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")


##### 2020 Census tracts #####
tract_2020_warehouse <- file.path(
  AWS_S3_WAREHOUSE_BUCKET, "spatial", "geojson", "tract-2020.geojson"
)
if (!aws.s3::object_exists(tract_2020_warehouse)) {
  tracts_2020 <- tigris::tracts(
    state = "17",
    county = "031",
    cb = TRUE,
    year = 2020
  )

  # Write geojson to S3
  tmp_file_geojson <- tempfile(fileext = ".geojson")
  st_write(tracts_2020, tmp_file_geojson)
  aws.s3::put_object(tmp_file_geojson, tract_2020_warehouse)
}
