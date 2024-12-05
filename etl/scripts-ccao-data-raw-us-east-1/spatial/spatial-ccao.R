library(aws.s3)
library(dplyr)
library(purrr)
library(sf)
library(stringr)
source("utils.R")

# This script retrieves CCAO neighborhood boundaries
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
output_bucket <- file.path(AWS_S3_RAW_BUCKET, "spatial", "ccao")

# Read privileges for the this drive location are limited.
# Contact Cook County GIS if permissions need to be changed.
file_path <- "//gisemcv1.ccounty.com/ArchiveServices/"

sources_list <- bind_rows(list(
  # NEIGHBORHOOD
  "neighborhood" = c(
    "url" = paste0(
      "https://gitlab.com/ccao-data-science---modeling/packages/ccao",
      "/-/raw/master/data-raw/nbhd_shp.geojson"
    ),
    "boundary" = "neighborhood",
    "year" = "2021"
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

# TOWNSHIPS

# Paths for all relevant geodatabases
gdb_files <- data.frame("path" = list.files(file_path, full.names = TRUE)) %>%
  filter(
    str_detect(path, "Current", negate = TRUE) &
      str_detect(path, "20") &
      # We detect parcel GDBs, but will extract the township layer
      str_detect(path, "Parcels")
  )

# Function to call referenced GDBs, pull requested data, and write it to S3
pwalk(gdb_files, function(...) {
  df <- tibble::tibble(...)
  county_gdb_to_s3(
    s3_bucket_uri = output_bucket,
    dir_name = "township",
    file_path = df$path,
    layer = "PoliticalTownship"
  )
})
