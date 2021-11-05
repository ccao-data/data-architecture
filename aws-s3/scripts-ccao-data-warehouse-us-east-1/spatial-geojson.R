library(aws.s3)
library(arrow)
library(dplyr)
library(purrr)
library(sf)
library(stringr)

# This script cleans geometry files and moves them to a specific folder for use
# in Tableau and other visualization software
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")


##### 2020 Census tracts #####
tract_2020_raw <- file.path(
  AWS_S3_RAW_BUCKET, "spatial", "census", "tract", "2020.geojson"
)
tract_2020_warehouse <- file.path(
  AWS_S3_WAREHOUSE_BUCKET, "spatial", "geojson", "tract-2020.geojson"
)
if (!aws.s3::object_exists(tract_2020_warehouse)) {
  tmp_file <- tempfile(fileext = ".geojson")
  aws.s3::save_object(tract_2020_raw, file = tmp_file)
  df <- sf::read_sf(tmp_file) %>%
    st_transform(4326) %>%
    select(starts_with(
      c("GEOID", "NAME", "INTPT", "ALAND", "AWATER"),
      ignore.case = TRUE
    )) %>%
    set_names(str_remove_all(names(.), "[[:digit:]]")) %>%
    st_drop_geometry() %>%
    st_as_sf(coords = c("INTPTLON", "INTPTLAT"), crs = st_crs(df)) %>%
    cbind(st_coordinates(.)) %>%
    st_drop_geometry() %>%
    select(lon = X, lat = Y) %>%
    cbind(df, .) %>%
    select(
      -starts_with("INTPT", ignore.case = TRUE),
      -ends_with("LSAD", ignore.case = TRUE)
    ) %>%
    rename_with(tolower)

  # Write geojson to S3
  tmp_file_geojson <- tempfile(fileext = ".geojson")
  st_write(df, tmp_file_geojson)
  aws.s3::put_object(tmp_file_geojson, tract_2020_warehouse)
}
