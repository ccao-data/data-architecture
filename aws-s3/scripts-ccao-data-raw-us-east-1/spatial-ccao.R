library(aws.s3)
library(dplyr)
library(purrr)
library(sf)
source("utils.R")

# This script retrieves CCAO neighborhood boundaries
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
output_bucket <- file.path(AWS_S3_RAW_BUCKET, "spatial", "ccao")

sources_list <- bind_rows(list(
  # NEIGHBORHOOD
  "neighborhood" = c(
    "url" = paste0(
      "https://gitlab.com/ccao-data-science---modeling/packages/ccao",
      "/-/raw/master/data-raw/nbhd_shp.geojson"
    ),
    "boundary" = "neighborhood",
    "year" = "2021"
  ),

  # TOWNSHIP
  "township" = c(
    "url" = paste0(
      "https://gitlab.com/ccao-data-science---modeling/packages/ccao",
      "/-/raw/master/data-raw/town_shp.geojson"
    ),
    "boundary" = "township",
    "year" = "2019"
  )
))

# Function to call referenced API, pull requested data, and write it to S3
pwalk(sources_list, function(...) {
  df <- tibble::tibble(...)
  open_data_to_s3(
    s3_bucket_uri = output_bucket,
    base_url = df$url,
    data_url = "",
    dir_name = df$boundary,
    file_year = df$year,
    file_ext = ".geojson"
  )
})