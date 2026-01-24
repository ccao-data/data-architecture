library(aws.s3)
library(dplyr)
library(purrr)
library(sf)
library(stringr)
source("utils.R")

AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
output_bucket <- file.path(AWS_S3_RAW_BUCKET, "spatial", "other")

# Read privileges for the this drive location are limited.
# Contact Cook County GIS if permissions need to be changed.
file_path <- "//gisemcv1.ccounty.com/ArchiveServices/"

sources_list <- bind_rows(list(
  # CHICAGO COMMUNITY AREA
  "cca_2018" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "cauq-8yn6?method=export&format=GeoJSON",
    "boundary" = "community_area",
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


##### SUBDIVISIONS #####
# Paths for all relevant geodatabases. This is a layer of a GDB file created and
# maintained by Cook County GIS.
data.frame("path" = list.files(file_path, full.names = TRUE)) %>%
  filter(
    str_detect(path, "Current", negate = TRUE) &
      str_detect(path, "20") &
      str_detect(path, "Parcel")
  ) %>%
  # Function to call referenced GDBs, pull requested data, and write it to S3
  pwalk(function(...) {
    df <- tibble::tibble(...)
    county_gdb_to_s3(
      s3_bucket_uri = output_bucket,
      dir_name = "subdivision",
      file_path = df$path,
      layer = "Subdivision"
    )
  })
