library(arrow)
library(aws.s3)
library(dplyr)
library(here)
library(purrr)
library(sf)
library(sfarrow)
library(stringr)
library(tidyr)

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
tmp_file_coastline <- tempfile(fileext = ".geojson")

if (!aws.s3::object_exists(remote_file_coastline_warehouse)) {

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

  # All we need to do is recode the special flood hazard area column from boolean to integer
  # And remove superfluous columns
  st_read(tmp_file) %>%
    select(SFHA_TF, geometry) %>%
    mutate(SFHA_TF = as.integer(as.logical(SFHA_TF))) %>%
    rename("fema_special_flood_hazard_area" = "SFHA_TF") %>%
    st_transform(3435) %>%
    sfarrow::st_write_parquet(flood_fema_warehouse)
  file.remove(tmp_file)

}