library(aws.s3)
library(dplyr)
library(osmdata)
library(purrr)
library(sf)
source("utils.R")

# Script to gather various sources of building footprint data
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
current_date <- Sys.Date()
current_year <- strftime(current_date, "%Y")
output_bucket <- file.path(
  AWS_S3_RAW_BUCKET, "spatial", "building_footprint"
)

##### OSM BUILDING FOOTPRINTS #####
remote_file_osm <- file.path(
  output_bucket, "osm", paste0(current_year, ".geojson")
)

if (!aws.s3::object_exists(remote_file_osm)) {
  tmp_file_osm <- tempfile(fileext = ".geojson")

  # Gather building footprints from OSM
  footprints <- opq("Cook County United States") %>%
    add_osm_feature(key = "building") %>%
    osmdata_sf()

  # Only need polygons
  footprints <- footprints$osm_polygons

  # Filter invalid geoms
  footprints %>%
    filter(!st_is_empty(geometry), st_is_valid(geometry)) %>%
    st_cast("MULTIPOLYGON") %>%
    mutate(pull_date = current_date) %>%
    select(osm_id, name, height, pull_date, geometry) %>%
    st_write(tmp_file_osm, delete_dsn = TRUE)

  # Upload to AWS
  save_local_to_s3(remote_file_osm, tmp_file_osm)
  file.remove(tmp_file_osm)
}


##### ESRI / COOK COUNTY FOOTPRINTS #####
# To check if data below has been updated navigate to
# https://datacatalog.cookcountyil.gov/resource/{asset id} where {asset id} is
# the 9-digit hyphenated asset identifier

sources_list <- bind_rows(list(
  "esri_sub" = c(
    "source" = "https://datacatalog.cookcountyil.gov/api/geospatial/",
    "api_url" = "dh3h-25vu?method=export&format=GeoJSON",
    "boundary" = "esri",
    "area" = "suburban_cook",
    "year" = "2008"
  ),
  "esri_chi" = c(
    "source" = "https://datacatalog.cookcountyil.gov/api/geospatial/",
    "api_url" = "7n8x-uj9u?method=export&format=GeoJSON",
    "boundary" = "esri",
    "area" = "chicago",
    "year" = "2008"
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
    file_ext = ".geojson",
    file_prefix = df$area
  )
})


##### MICROSOFT FOOTPRINTS #####
remote_file_microsoft <- file.path(
  output_bucket, "microsoft", "2019.geojson"
)

if (!aws.s3::object_exists(remote_file_microsoft)) {
  tmp_file_zip <- tempfile(fileext = ".zip")
  ext_file <- file.path(tempdir(), "Illinois.geojson")

  # Download compressed IL footprints file
  download.file(
    paste0(
      "https://usbuildingdata.blob.core.windows.net",
      "/usbuildings-v2/Illinois.geojson.zip"
    ),
    tmp_file_zip
  )
  unzip(tmp_file_zip, exdir = dirname(ext_file))
  save_local_to_s3(remote_file_microsoft, ext_file)
  file.remove(ext_file)
}
