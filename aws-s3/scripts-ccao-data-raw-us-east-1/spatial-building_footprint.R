library(aws.s3)
library(dplyr)
library(osmdata)
library(sf)

# Script to gather various sources of building footprint data
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
current_year <- strftime(Sys.Date(), "%Y")
current_date <- Sys.Date()


# OSM BUILDING FOOTPRINTS
remote_file_osm <- file.path(
  AWS_S3_RAW_BUCKET, "spatial", "building_footprint",
  "osm", paste0(current_year, ".geojson"))

if (!aws.s3::object_exists(remote_file_osm)) {

  temp_file_osm <- tempfile(fileext = ".geojson")

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

    # Upload to AWS
    st_write(temp_file_osm, delete_dsn = TRUE)

  aws.s3::put_object(temp_file_osm, remote_file_osm, multipart = TRUE)
  file.remove(temp_file_osm)
}


# ESRI / COOK COUNTY FOOTPRINTS
api_info <- list(
  "esri_sub" = c("source"   = "https://datacatalog.cookcountyil.gov/api/geospatial/",
                 "api_url"  = "dh3h-25vu?method=export&format=GeoJSON",
                 "boundary" = "esri",
                 "area"     = "suburban_cook",
                 "year"     = "2008"),

  "esri_chi" = c("source"   = "https://datacatalog.cookcountyil.gov/api/geospatial/",
                 "api_url"  = "7n8x-uj9u?method=export&format=GeoJSON",
                 "boundary" = "esri",
                 "area"     = "chicago",
                 "year"     = "2008")
)

# Function to call referenced API, pull requested data, and write it to S3
pull_and_write <- function(x) {

  tmp_file <- tempfile(fileext = ".geojson")
  remote_file <- file.path(
    AWS_S3_RAW_BUCKET, "spatial", "building_footprint",
    x["boundary"], paste0(x["area"], "-", x["year"], ".geojson")
  )

  if (!aws.s3::object_exists(remote_file)) {
    download.file(paste0(x["source"], x["api_url"]), tmp_file)
    aws.s3::put_object(tmp_file, remote_file)
    file.remove(tmp_file)
  }
}

# Apply function to "api_info"
lapply(api_info, pull_and_write)


# MICROSOFT FOOTPRINTS
remote_file_microsoft <- file.path(
  AWS_S3_RAW_BUCKET, "spatial", "building_footprint",
  "microsoft", "2019.geojson")

if (!aws.s3::object_exists(remote_file_microsoft)) {

  temp_file_zip <- tempfile(fileext = ".zip")
  ext_file <- file.path(tempdir(), "Illinois.geojson")

  # Download compressed IL footprints file
  download.file(
    paste0(
      "https://usbuildingdata.blob.core.windows.net",
      "/usbuildings-v2/Illinois.geojson.zip"
    ),
    temp_file_zip
  )
  unzip(temp_file_zip, exdir = dirname(ext_file))
  aws.s3::put_object(ext_file, remote_file_microsoft, multipart = TRUE)
  file.remove(ext_file)
}