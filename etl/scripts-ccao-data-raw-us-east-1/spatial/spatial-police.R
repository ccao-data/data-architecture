library(aws.s3)
library(dplyr)
library(purrr)
library(sf)
source("utils.R")

# This script retrieves police districts
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
output_bucket <- file.path(AWS_S3_RAW_BUCKET, "spatial", "police")

sources_list <- bind_rows(list(
  # POLICE
  # To check if this data has been updated, visit:
  # https://data.cityofchicago.org/Public-Safety/PoliceDistrictDec2012/24zt-jpfn/about_data # nolint
  "pol_2012" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "p3h8-xsd4?method=export&format=GeoJSON",
    "boundary" = "police_district",
    "year" = "2012"
  ),
  "pol_2018" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "fthy-xz3r?method=export&format=GeoJSON",
    "boundary" = "police_district",
    "year" = "2018"
  )
))

# Function to call referenced API, pull requested data, and write it to S3
pwalk(sources_list, function(...) {
  df <- tibble::tibble(...)
  open_data_to_s3(
    s3_bucket_uri = output_bucket,
    base_url = df$source,
    data_url = df$api_url,
    dir_name = df$boundary,
    file_year = df$year,
    file_ext = ".geojson"
  )
})
