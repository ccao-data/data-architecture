library(arrow)
library(aws.s3)
library(dplyr)
library(here)
library(purrr)
library(sf)
library(sfarrow)
library(stringr)
library(tidyr)
source("utils.R")

# This script cleans environmental data such as floodplains and coastal
# boundaries and uploads them to S3 as parquet files
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
current_year <- strftime(Sys.Date(), "%Y")

##### LAKE MICHICAN COASTLINE #####
remote_file_coastline_raw <- file.path(
  AWS_S3_RAW_BUCKET, "spatial", "environment", "coastline",
  paste0("2021.geojson")
)
remote_file_coastline_warehouse <- file.path(
  AWS_S3_WAREHOUSE_BUCKET, "spatial", "environment", "coastline",
  paste0("2021.geojson")
)

if (!aws.s3::object_exists(remote_file_coastline_warehouse)) {
  tmp_file_coastline <- tempfile(fileext = ".geojson")
  aws.s3::save_object(remote_file_coastline_raw, file = tmp_file_coastline)

  # We need to clip the coastlines to only include Cook County
  cook_boundary <- st_read(
    paste0(
      "https://opendata.arcgis.com/datasets/",
      "ea127f9e96b74677892722069c984198_1.geojson"
    )
  ) %>%
    st_transform(4326) %>%
    st_buffer(1)

  st_read(tmp_file_coastline) %>%
    st_transform(4326) %>%
    filter(as.logical(st_intersects(geometry, cook_boundary))) %>%
    mutate(
      NAME = "Lake Michigan",
      geometry_3435 = st_transform(geometry, 3435)
    ) %>%
    rename_with(tolower) %>%
    sfarrow::st_write_parquet(remote_file_coastline_warehouse)
}


##### FEMA FLOODPLAINS #####
flood_fema_raw <- file.path(
  AWS_S3_RAW_BUCKET, "spatial", "environment", "flood_fema",
  paste0(current_year, ".geojson")
)

flood_fema_warehouse <- file.path(
  AWS_S3_WAREHOUSE_BUCKET, "spatial", "environment", "flood_fema",
  paste0(current_year, ".parquet")
)

# Write FEMA floodplains to S3 if they don't exist
if (aws.s3::object_exists(flood_fema_raw) & !aws.s3::object_exists(flood_fema_warehouse)) {
  tmp_file <- tempfile(fileext = ".geojson")
  aws.s3::save_object(flood_fema_raw, file = tmp_file)

  # All we need to do is recode the special flood hazard area column
  # And remove superfluous columns
  st_read(tmp_file) %>%
    st_transform(4326) %>%
    mutate(
      SFHA_TF = as.logical(SFHA_TF),
      geometry_3435 = st_transform(geometry, 3435)
    ) %>%
    select(
      fema_special_flood_hazard_area = SFHA_TF,
      geometry, geometry_3435
    ) %>%
    sfarrow::st_write_parquet(flood_fema_warehouse)
  file.remove(tmp_file)
}


##### RAILROAD #####
remote_file_rail_raw <- file.path(
  AWS_S3_RAW_BUCKET, "spatial", "environment", "railroad",
  paste0("2021.geojson")
)
remote_file_rail_warehouse <- file.path(
  AWS_S3_WAREHOUSE_BUCKET, "spatial", "environment", "railroad",
  paste0("2021.geojson")
)

if (!aws.s3::object_exists(remote_file_rail_warehouse)) {
  tmp_file_rail <- tempfile(fileext = ".geojson")
  aws.s3::save_object(remote_file_rail_raw, file = tmp_file_rail)

  st_read(tmp_file_rail) %>%
    st_transform(4326) %>%
    rename_with(tolower) %>%
    mutate(
      geometry_3435 = st_transform(geometry, 3435)
    ) %>%
    select(
      name_id,
      unique_id = uniqid, name = anno_name, taxmap_id, type,
      ortho_year = orthoyear, commute_line = commute_li,
      geometry, geometry_3435
    ) %>%
    sfarrow::st_write_parquet(remote_file_rail_warehouse)
}

##### HYDROLOGY #####
raw_files_hydro <- grep(
  "geojson",
  file.path(
    AWS_S3_RAW_BUCKET,
    get_bucket_df(AWS_S3_RAW_BUCKET, prefix = "spatial/environment/hydrology/")$Key
  ),
  value = TRUE
)

dest_files_hydro <- raw_files_hydro %>%
  gsub("geojson", "parquet", .) %>%
  gsub(AWS_S3_RAW_BUCKET, AWS_S3_WAREHOUSE_BUCKET, .)

# Function to pull raw data from S3 and clean
clean_hydro <- function(remote_file, dest_file) {
  if (!aws.s3::object_exists(dest_file)) {
    tmp_file <- tempfile(fileext = ".geojson")
    aws.s3::save_object(remote_file, file = tmp_file)

    st_read(tmp_file) %>%
      select(id = HYDROID, name = FULLNAME, geometry) %>%
      mutate(geometry_3435 = st_transform(geometry, 3435)) %>%
      sfarrow::st_write_parquet(dest_file)

    file.remove(tmp_file)
  }
}

# Apply function to raw_files
mapply(clean_hydro, raw_files_hydro, dest_files_hydro)
