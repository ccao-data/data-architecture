library(aws.s3)
library(dplyr)
library(purrr)
library(sf)
source("utils.R")

# This script retrieves major political boundaries such as townships and
# judicial districts
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
output_bucket <- file.path(AWS_S3_RAW_BUCKET, "spatial", "political")

sources_list <- bind_rows(list(
  # BOARD OF REVIEW
  "bor_2012" = c(
    "source" = "https://datacatalog.cookcountyil.gov/api/geospatial/",
    "api_url" = "7iyt-i4z4?method=export&format=GeoJSON",
    "boundary" = "board_of_review",
    "year" = "2012"
  ),

  # COMMISSIONER DISTRICT
  "cmd_2012" = c(
    "source" = "https://datacatalog.cookcountyil.gov/api/geospatial/",
    "api_url" = "ihae-id2m?method=export&format=GeoJSON",
    "boundary" = "commissioner",
    "year" = "2012"
  ),

  # TOWNSHIPS
  "twn_2019" = c(
    "source" = "https://opendata.arcgis.com/datasets/",
    "api_url" = "78fe09c5954e41e19b65a4194eed38c7_3.geojson",
    "boundary" = "township",
    "year" = "2019"
  ),

  # CONGRESSIONAL DISTRICT
  "cnd_2010" = c(
    "source" = "https://datacatalog.cookcountyil.gov/api/geospatial/",
    "api_url" = "jh56-md8x?method=export&format=GeoJSON",
    "boundary" = "congressional_district",
    "year" = "2010"
  ),

  # JUDICIAL SUBCIRCUIT DISTRICT
  "jsd_2012" = c(
    "source" = "https://datacatalog.cookcountyil.gov/api/geospatial/",
    "api_url" = "54r3-ikn3?method=export&format=GeoJSON",
    "boundary" = "judicial",
    "year" = "2012"
  ),

  # MUNICIPALITY
  "mnc_2021" = c(
    "source" = "https://opendata.arcgis.com/datasets/",
    "api_url" = "534226c6b1034985aca1e14a2eb234af_2.geojson",
    "boundary" = "municipality",
    "year" = "2021"
  ),

  # STATE REPRESENTATIVE
  "str_2010" = c(
    "source" = "https://datacatalog.cookcountyil.gov/api/geospatial/",
    "api_url" = "gsew-ir9y?method=export&format=GeoJSON",
    "boundary" = "state_representative",
    "year" = "2010"
  ),

  # STATE SENATE
  "sts_2010" = c(
    "source" = "https://datacatalog.cookcountyil.gov/api/geospatial/",
    "api_url" = "ezne-sr8y?method=export&format=GeoJSON",
    "boundary" = "state_senate",
    "year" = "2010"
  ),

  # WARD
  "wrd_2003" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "xt4z-bnwh?method=export&format=GeoJSON",
    "boundary" = "ward",
    "year" = "2003"
  ),
  "wrd_2015" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "sp34-6z76?method=export&format=GeoJSON",
    "boundary" = "ward",
    "year" = "2015"
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