library(arrow)
library(aws.s3)
library(dplyr)
library(purrr)
library(sf)
library(sfarrow)
library(stars)
library(stringr)
library(tidyr)
source("utils.R")

# This scripts cleans and uploads airport noise surface data
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(
  AWS_S3_WAREHOUSE_BUCKET,
  "environment", "airport_noise"
)

# Location of raster files
raw_files <- grep(
  "tif",
  file.path(
    AWS_S3_RAW_BUCKET,
    get_bucket_df(
      AWS_S3_RAW_BUCKET,
      prefix = "environment/airport_noise"
    )$Key
  ),
  value = TRUE
)
raw_files_omp <- grep("omp_19", raw_files, value = TRUE)
raw_files_no_omp <- grep("omp_19", raw_files, value = TRUE, invert = TRUE)


# Function to merge raster with PINs for each year then save back to S3
merge_pins_with_raster <- function(raw_file) {
  tmp_file <- tempfile(fileext = ".tif")
  aws.s3::save_object(raw_file, file = tmp_file)
  year <- str_extract(raw_file, "[0-9]{4}")
  message("Now processing:", year)

  rast <- stars::read_stars(tmp_file)
  pins <- sfarrow::read_sf_dataset(arrow::open_dataset(paste0(
    "s3://ccao-data-warehouse-us-east-1/spatial/parcel/year=",
    year
  ))) %>%
    st_set_geometry(.$geometry_3435)

  pins %>%
    mutate(
      airport_noise_dnl = st_extract(rast, st_centroid(.)) %>%
        st_drop_geometry() %>%
        pull()
    ) %>%
    select(pin10, airport_noise_dnl) %>%
    mutate(airport_noise_dnl = replace_na(airport_noise_dnl, 52.5)) %>%
    st_drop_geometry() %>%
    mutate(loaded_at = as.character(Sys.time())) %>%
    write_parquet(
      file.path(output_bucket, paste0("year=", year), "part-0.parquet")
    )
}


# Walk through each year of merges and upload to S3
purrr::walk(raw_files_no_omp, merge_pins_with_raster)

# Extra code for merging years after 2020
tmp_file <- tempfile(fileext = ".tif")
aws.s3::save_object(raw_files_omp, file = tmp_file)
message("Now processing:OMP")

rast <- stars::read_stars(tmp_file)
pins <- read_sf_dataset(arrow::open_dataset(paste0(
  "s3://ccao-data-warehouse-us-east-1/spatial/parcel/year=2021"
))) %>%
  st_set_geometry(.$geometry_3435)

pins %>%
  mutate(
    airport_noise_dnl = st_extract(rast, st_centroid(.)) %>%
      st_drop_geometry() %>%
      pull()
  ) %>%
  select(pin10, airport_noise_dnl) %>%
  mutate(airport_noise_dnl = replace_na(airport_noise_dnl, 52.5)) %>%
  st_drop_geometry() %>%
  mutate(loaded_at = as.character(Sys.time())) %>%
  write_parquet(
    file.path(output_bucket, paste0("year=omp"), "part-0.parquet")
  )
