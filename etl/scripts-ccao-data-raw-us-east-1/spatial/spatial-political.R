library(aws.s3)
library(dplyr)
library(purrr)
library(sf)
library(stringr)
source("utils.R")

# This script retrieves major political boundaries such as townships and
# judicial districts
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
output_bucket <- file.path(AWS_S3_RAW_BUCKET, "spatial", "political")

# Read privileges for the this drive location are limited.
# Contact Cook County GIS if permissions need to be changed.
file_path <- "//gisemcv1.ccounty.com/ArchiveServices/"

sources_list <- bind_rows(list(
  # BOARD OF REVIEW
  "bor_2012" = c(
    "source" = "https://datacatalog.cookcountyil.gov/api/geospatial/",
    "api_url" = "7iyt-i4z4?method=export&format=GeoJSON",
    "boundary" = "board_of_review_district",
    "year" = "2012"
  ),
  "bor_2023" = c(
    "source" = "https://gis.cookcountyil.gov/traditional/rest/services/politicalBoundary/MapServer/",
    "api_url" = "10/query?outFields=*&where=1%3D1&f=geojson",
    "boundary" = "board_of_review_district",
    "year" = "2023"
  ),

  # COMMISSIONER DISTRICT
  "cmd_2012" = c(
    "source" = "https://datacatalog.cookcountyil.gov/api/geospatial/",
    "api_url" = "ihae-id2m?method=export&format=GeoJSON",
    "boundary" = "commissioner_district",
    "year" = "2012"
  ),
  "cmd_2023" = c(
    "source" = "https://gis.cookcountyil.gov/traditional/rest/services/politicalBoundary/MapServer/",
    "api_url" = "9/query?outFields=*&where=1%3D1&f=geojson",
    "boundary" = "commissioner_district",
    "year" = "2023"
  ),

  # CONGRESSIONAL DISTRICT
  "cnd_2010" = c(
    "source" = "https://datacatalog.cookcountyil.gov/api/geospatial/",
    "api_url" = "jh56-md8x?method=export&format=GeoJSON",
    "boundary" = "congressional_district",
    "year" = "2010"
  ),
  "cnd_2023" = c(
    "source" = "https://gis.cookcountyil.gov/traditional/rest/services/politicalBoundary/MapServer/",
    "api_url" = "13/query?outFields=*&where=1%3D1&f=geojson",
    "boundary" = "congressional_district",
    "year" = "2023"
  ),

  # JUDICIAL SUBCIRCUIT DISTRICT
  "jsd_2012" = c(
    "source" = "https://datacatalog.cookcountyil.gov/api/geospatial/",
    "api_url" = "54r3-ikn3?method=export&format=GeoJSON",
    "boundary" = "judicial_district",
    "year" = "2012"
  ),
  "jsd_2022" = c(
    "source" = "https://gis.cookcountyil.gov/traditional/rest/services/politicalBoundary/MapServer/",
    "api_url" = "5/query?outFields=*&where=1%3D1&f=geojson",
    "boundary" = "judicial_district",
    "year" = "2022"
  ),

  # STATE REPRESENTATIVE
  "str_2010" = c(
    "source" = "https://datacatalog.cookcountyil.gov/api/geospatial/",
    "api_url" = "gsew-ir9y?method=export&format=GeoJSON",
    "boundary" = "state_representative_district",
    "year" = "2010"
  ),
  "str_2023" = c(
    "source" = "https://gis.cookcountyil.gov/traditional/rest/services/politicalBoundary/MapServer/",
    "api_url" = "11/query?outFields=*&where=1%3D1&f=geojson",
    "boundary" = "state_representative_district",
    "year" = "2023"
  ),

  # STATE SENATE
  "sts_2010" = c(
    "source" = "https://datacatalog.cookcountyil.gov/api/geospatial/",
    "api_url" = "ezne-sr8y?method=export&format=GeoJSON",
    "boundary" = "state_senate_district",
    "year" = "2010"
  ),
  "sts_2023" = c(
    "source" = "https://gis.cookcountyil.gov/traditional/rest/services/politicalBoundary/MapServer/",
    "api_url" = "12/query?outFields=*&where=1%3D1&f=geojson",
    "boundary" = "state_senate_district",
    "year" = "2023"
  ),

  # CHICAGO WARD
  "cwrd_2003" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "xt4z-bnwh?method=export&format=GeoJSON",
    "boundary" = "ward_chicago",
    "year" = "2003"
  ),
  "cwrd_2015" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "sp34-6z76?method=export&format=GeoJSON",
    "boundary" = "ward_chicago",
    "year" = "2015"
  ),
  "cwrd_2023" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "p293-wvbd?method=export&format=GeoJSON",
    "boundary" = "ward_chicago",
    "year" = "2023"
  ),

  # EVANSTON WARD
  "ewrd_2019" = c(
    "source" = "https://data.cityofevanston.org/api/geospatial/",
    "api_url" = "42py-uyez?method=export&format=GeoJSON",
    "boundary" = "ward_evanston",
    "year" = "2019"
  ),
  "ewrd_2022" = c(
    "source" = "https://data.cityofevanston.org/api/geospatial/",
    "api_url" = "qqzu-c4r8?method=export&format=GeoJSON",
    "boundary" = "ward_evanston",
    "year" = "2022"
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

# MUNICIPALITY

# Paths for all relevant geodatabases
data.frame("path" = list.files(file_path, full.names = TRUE)) %>%
  filter(
    str_detect(path, "Current", negate = TRUE) &
      str_detect(path, "20") &
      str_detect(path, "Admin")
  ) %>%

# Function to call referenced API, pull requested data, and write it to S3
pwalk(function(...) {
  df <- tibble::tibble(...)
  county_gdb_to_s3(
    s3_bucket_uri = output_bucket,
    dir_name = "municipality",
    file_path = df$path,
    layer = "MuniTaxDist"
  )
})
