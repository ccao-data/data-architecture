library(arrow)
library(aws.s3)
library(dplyr)
library(here)
library(purrr)
library(sf)
library(sfarrow)
library(stringr)
library(tidyr)

# This script cleans shapefiles that represent desirable amenities, such as
# parks and hospitals
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
current_year <- strftime(Sys.Date(), "%Y")


##### BIKE TRAIL #####
remote_file_bike_raw <- file.path(
  AWS_S3_RAW_BUCKET, "spatial", "access", "bike_trail",
  paste0("2021.geojson")
)
remote_file_bike_warehouse <- file.path(
  AWS_S3_WAREHOUSE_BUCKET, "spatial", "access", "bike_trail",
  paste0("2021.geojson")
)

if (!aws.s3::object_exists(remote_file_bike_warehouse)) {

  tmp_file_bike <- tempfile(fileext = ".geojson")
  aws.s3::save_object(remote_file_bike_raw, file = tmp_file_bike)

  st_read(tmp_file_bike) %>%
    st_transform(4326) %>%
    rename_with(tolower) %>%
    mutate(
      geometry_3435 = st_transform(geometry, 3435)
    ) %>%
    select(-c(created_us:shape_stle)) %>%
    rename(
      speed_limit = spdlimit, on_street = onstreet, edit_date = edtdate,
      trail_width = trailwdth, trail_type = trailtype,
      trail_surface = trailsurfa
    ) %>%
    sfarrow::st_write_parquet(remote_file_bike_warehouse)
}


##### CEMETERY #####
remote_file_ceme_raw <- file.path(
  AWS_S3_RAW_BUCKET, "spatial", "access", "cemetery",
  paste0("2021.geojson")
)
remote_file_ceme_warehouse <- file.path(
  AWS_S3_WAREHOUSE_BUCKET, "spatial", "access", "cemetery",
  paste0("2021.geojson")
)

if (!aws.s3::object_exists(remote_file_ceme_warehouse)) {

  tmp_file_ceme <- tempfile(fileext = ".geojson")
  aws.s3::save_object(remote_file_ceme_raw, file = tmp_file_ceme)

  st_read(tmp_file_ceme) %>%
    st_transform(4326) %>%
    rename_with(tolower) %>%
    mutate(
      geometry_3435 = st_transform(geometry, 3435)
    ) %>%
    select(
      name = cfname, address, gniscode, source, community, comment, mergeid,
      geometry, geometry_3435
    ) %>%
    sfarrow::st_write_parquet(remote_file_ceme_warehouse)
}


##### HOSPITAL #####
remote_file_hosp_raw <- file.path(
  AWS_S3_RAW_BUCKET, "spatial", "access", "hospital",
  paste0("2021.geojson")
)
remote_file_hosp_warehouse <- file.path(
  AWS_S3_WAREHOUSE_BUCKET, "spatial", "access", "hospital",
  paste0("2021.geojson")
)

if (!aws.s3::object_exists(remote_file_hosp_warehouse)) {

  tmp_file_hosp <- tempfile(fileext = ".geojson")
  aws.s3::save_object(remote_file_hosp_raw, file = tmp_file_hosp)

  st_read(tmp_file_hosp) %>%
    st_transform(4326) %>%
    rename_with(tolower) %>%
    mutate(
      geometry_3435 = st_transform(geometry, 3435)
    ) %>%
    select(
      name = cfname, address, gniscode, source, community, comment, mergeid,
      geometry, geometry_3435
    ) %>%
    sfarrow::st_write_parquet(remote_file_hosp_warehouse)
}


##### PARK #####
remote_file_park_raw <- file.path(
  AWS_S3_RAW_BUCKET, "spatial", "access", "park",
  paste0("2021.geojson")
)
remote_file_park_warehouse <- file.path(
  AWS_S3_WAREHOUSE_BUCKET, "spatial", "access", "park",
  paste0("2021.geojson")
)

if (!aws.s3::object_exists(remote_file_park_warehouse)) {

  tmp_file_park <- tempfile(fileext = ".geojson")
  aws.s3::save_object(remote_file_park_raw, file = tmp_file_park)

  st_read(tmp_file_park) %>%
    st_transform(4326) %>%
    rename_with(tolower) %>%
    mutate(
      geometry_3435 = st_transform(geometry, 3435)
    ) %>%
    select(
      name = cfname, address, gniscode, source, community, jurisdiction,
      comment, geometry, geometry_3435
    ) %>%
    sfarrow::st_write_parquet(remote_file_park_warehouse)
}


##### INDUSTRIAL CORRIDOR #####
remote_file_indc_raw <- file.path(
  AWS_S3_RAW_BUCKET, "spatial", "access", "industrial_corridor",
  paste0("2013.geojson")
)
remote_file_indc_warehouse <- file.path(
  AWS_S3_WAREHOUSE_BUCKET, "spatial", "access", "industrial_corridor",
  paste0("2013.geojson")
)

if (!aws.s3::object_exists(remote_file_indc_warehouse)) {

  tmp_file_indc <- tempfile(fileext = ".geojson")
  aws.s3::save_object(remote_file_indc_raw, file = tmp_file_indc)

  st_read(tmp_file_indc) %>%
    st_transform(4326) %>%
    rename_with(tolower) %>%
    mutate(
      geometry_3435 = st_transform(geometry, 3435)
    ) %>%
    select(
      name, region, num = no, hud_qualif, acres,
      geometry, geometry_3435
    ) %>%
    sfarrow::st_write_parquet(remote_file_indc_warehouse)
}