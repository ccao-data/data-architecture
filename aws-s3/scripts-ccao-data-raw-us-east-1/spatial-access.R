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
    "year" = "2014"
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
raw_walk <- c(
  "url" = "https://datahub.cmap.illinois.gov/dataset/aac0d840-77b4-4e88-8a26-7220ac6c588f/resource/8f8ff761-0c02-41e1-9877-432fd0f42b07/download/Walkability.zip",
  "year" = "2017")

get_walkability <- function(url, year) {

  s3_uri <- file.path(output_bucket, "walkability", paste0(year, ".geojson"))

  if (!aws.s3::object_exists(s3_uri)) {
    tmp_file <- tempfile(fileext = ".zip")
    tmp_dir <- file.path(tempdir(), "walkability")

    # Grab file from CTA, recompress without .htm file
    download.file(url, destfile = tmp_file, mode = "wb")
    unzip(tmp_file, exdir = tmp_dir)
    tmp_file <- file.path(tmp_dir, paste0(year, ".geojson"))
    if (file.exists(tmp_file)) file.remove(tmp_file)
    st_read(
      grep("xml",
           grep("shp",
                list.files(tmp_dir, recursive = TRUE, full.names = TRUE),
                value = TRUE),
           invert = TRUE,
           value = TRUE)
    ) %>%
      st_write(tmp_file)
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
