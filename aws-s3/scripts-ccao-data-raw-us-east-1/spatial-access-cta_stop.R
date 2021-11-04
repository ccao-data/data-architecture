library(aws.s3)
library(dplyr)
library(sf)
library(tidytransit)
library(zip)

# This script retrieves location data for all CTA bus and train stops
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
bkt_path <- file.path(AWS_S3_RAW_BUCKET, "spatial", "access", "cta_stop")
current_year <- strftime(Sys.Date(), "%Y")

# Paths to files on S3
remote_file_feed <- file.path(bkt_path, "feed", paste0(current_year, ".zip"))
remote_file_train <- file.path(bkt_path, "train", paste0(current_year, ".geojson"))
remote_file_bus <- file.path(bkt_path, "bus", paste0(current_year, ".geojson"))

# If missing feed on S3, download and remove .htm file (causes errors)
# then rezip and upload
if (!aws.s3::object_exists(remote_file_feed)) {
  tmp_file <- tempfile(fileext = ".zip")
  tmp_dir <- tempdir()

  # Grab file from CTA, recompress without .htm file
  download.file(
    "http://www.transitchicago.com/downloads/sch_data/google_transit.zip",
    destfile = tmp_file, mode = "wb"
  )
  unzip(tmp_file, exdir = tmp_dir)
  zip::zipr(
    zipfile = tmp_file,
    files = list.files(tmp_dir, full.names = TRUE, pattern = ".txt")
  )
  aws.s3::put_object(tmp_file, remote_file_feed)
  file.remove(tmp_file)
}

# Grab saved feed from S3
tmp_feed <- aws.s3::save_object(
  remote_file_feed,
  file = tempfile(fileext = ".zip")
)

# Upload train stops if missing
if (!aws.s3::object_exists(remote_file_train)) {

  # Save train stops
  tmp_file_train <- tempfile(fileext = ".geojson")
  read_gtfs(tmp_feed) %>%
    filter_stops(
      route_ids = .$routes %>% filter(route_type == 1) %>% pull(route_id),
      service_ids = .$calendar %>% pull(service_id)
    ) %>%

    # Make sure data is unique by stop
    distinct(stop_name, .keep_all = TRUE) %>%
    st_as_sf(coords = c("stop_lon", "stop_lat")) %>%

    # Write data as geojson
    st_write(tmp_file_train, delete_dsn = TRUE)
  aws.s3::put_object(tmp_file_train, remote_file_train)
  file.remove(tmp_file_train)
}

# Upload bus stops if missing
if (!aws.s3::object_exists(remote_file_bus)) {

  # Save bus stops
  tmp_file_bus <- tempfile(fileext = ".geojson")
  read_gtfs(tmp_feed) %>%
    filter_stops(
      route_ids = .$routes %>% filter(route_type == 3) %>% pull(route_id),
      service_ids = .$calendar %>% pull(service_id)
    ) %>%

    # Make sure data is unique by stop
    distinct(stop_name, .keep_all = TRUE) %>%
    st_as_sf(coords = c("stop_lon", "stop_lat")) %>%

    # Write data as geojson
    st_write(tmp_file_bus, delete_dsn = TRUE)
  aws.s3::put_object(tmp_file_bus, remote_file_bus)
  file.remove(tmp_file_bus)
}

# Cleanup
file.remove(tmp_feed)
rm(list = ls())