library(aws.s3)
library(dplyr)
library(purrr)
library(sf)
source("utils.R")

# This script retrieves the spatial/location data for
# industrial corridors in the City of Chicago
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
output_bucket <- file.path(AWS_S3_RAW_BUCKET, "spatial", "access")

# List APIs from city site
sources_list <- bind_rows(list(
  # INDUSTRIAL CORRIDORS
  # See https://data.cityofchicago.org/Community-Economic-Development/IndustrialCorridor_Jan2013/3tu3-iesz/about_data
  # for more information
  "ind_2013" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "e6xh-nr8w?method=export&format=GeoJSON",
    "boundary" = "industrial_corridor",
    "year" = "2013"
  ),

  # PARKS
  "prk_2021" = c(
    "source" = "https://opendata.arcgis.com/datasets/",
    "api_url" = "74d19d6bd7f646ecb34c252ae17cd2f7_7.geojson",
    "boundary" = "park",
    "year" = "2021"
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

##### CMAP WALKABILITY #####
# 2017 Data is no longer available online
raw_walk <- data.frame(
  "url" = "https://services5.arcgis.com/LcMXE3TFhi1BSaCY/arcgis/rest/services/Walkability/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson",
  "year" = "2018")

get_walkability <- function(url, year) {

  s3_uri <- file.path(output_bucket, "walkability", paste0(year, ".geojson"))

  if (!aws.s3::object_exists(s3_uri)) {

    tmp_file <- tempfile(fileext = ".geojson")
    tmp_dir <- file.path(tempdir(), "walkability")

    # Grab file from CTA, recompress without .htm file
    st_read(url) %>% st_write(tmp_file)
    save_local_to_s3(s3_uri, tmp_file, overwrite = TRUE)
    unlink(gsub("/walkability", "", tmp_dir))
  }
}

pwalk(raw_walk, function(...) {
  df <- tibble::tibble(...)
  get_walkability(
    url = df$url,
    year = df$year
  )
})
