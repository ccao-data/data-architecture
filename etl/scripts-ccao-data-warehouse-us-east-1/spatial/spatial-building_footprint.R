library(arrow)
library(aws.s3)
library(DBI)
library(dplyr)
library(geoarrow)
library(glue)
library(here)
library(noctua)
library(purrr)
library(sf)
library(stringr)
library(tidyr)
source("utils.R")

# This script cleans building footprint data from various sources and
# compiles it into a single table on AWS Athena
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
input_bucket <- file.path(
  AWS_S3_RAW_BUCKET, "spatial", "building_footprint"
)
output_bucket <- file.path(
  AWS_S3_WAREHOUSE_BUCKET, "spatial", "building_footprint"
)
footprint_tmp_dir <- here("footprint-tmp")

##### ESRI #####
esri_chicago_geojson <- here(footprint_tmp_dir, "chicago-2008.geojson")
esri_chicago_remote_raw <- file.path(
  input_bucket, "esri", "chicago-2008.parquet"
)
esri_chicago_remote <- file.path(
  output_bucket, "source=esri", "chicago-2008.parquet"
)

if (!aws.s3::object_exists(esri_chicago_remote)) {
  if (!file.exists(esri_chicago_geojson)) {
    aws.s3::save_object(esri_chicago_remote_raw, file = esri_chicago_geojson)
  }
  esri_chicago_df <- st_read(esri_chicago_geojson)
  esri_chicago_df_clean <- esri_chicago_df %>%
    st_transform(4326) %>%
    st_cast("MULTIPOLYGON") %>%
    mutate(
      year = 2008,
      geometry_3435 = st_transform(geometry, 3435),
      centroid = st_centroid(geometry_3435),
      area = st_area(geometry_3435)
    ) %>%
    cbind(
      st_coordinates(st_transform(.$centroid, 4326)),
      st_coordinates(.$centroid)
    ) %>%
    select(
      year,
      lon = X, lat = Y, x_3435 = X.1, y_3435 = Y.1,
      geometry, geometry_3435
    )
  geoparquet_to_s3(
    esri_chicago_df_clean,
    esri_chicago_remote
  )
}

esri_sub_geojson <- here(footprint_tmp_dir, "suburban_cook-2008.geojson")
esri_sub_remote_raw <- file.path(
  input_bucket, "esri", "suburban_cook-2008.parquet"
)
esri_sub_remote <- file.path(
  output_bucket, "source=esri", "suburban_cook-2008.parquet"
)

if (!aws.s3::object_exists(esri_sub_remote)) {
  if (!file.exists(esri_sub_geojson)) {
    aws.s3::save_object(esri_sub_remote_raw, file = esri_sub_geojson)
  }
  esri_sub_df <- st_read(esri_sub_geojson)
  esri_sub_df_clean <- esri_sub_df %>%
    st_transform(4326) %>%
    st_cast("MULTIPOLYGON") %>%
    mutate(
      year = 2008,
      geometry_3435 = st_transform(geometry, 3435),
      centroid = st_centroid(geometry_3435),
      area = st_area(geometry_3435)
    ) %>%
    cbind(
      st_coordinates(st_transform(.$centroid, 4326)),
      st_coordinates(.$centroid)
    ) %>%
    select(
      year,
      lon = X, lat = Y, x_3435 = X.1, y_3435 = Y.1,
      geometry, geometry_3435
    )
  geoparquet_to_s3(esri_sub_df_clean, esri_sub_remote)
}


##### OSM #####
osm_geojson <- here(footprint_tmp_dir, "osm-2021.geojson")
osm_remote_raw <- file.path(input_bucket, "osm", "2021.geojson")
osm_remote <- file.path(output_bucket, "source=osm", "osm-2021.parquet")

if (!aws.s3::object_exists(osm_remote)) {
  if (!file.exists(osm_geojson)) {
    aws.s3::save_object(osm_remote_raw, file = osm_geojson)
  }
  osm_df <- st_read(osm_geojson)
  osm_df_clean <- osm_df %>%
    st_transform(4326) %>%
    st_cast("MULTIPOLYGON") %>%
    mutate(
      year = 2021,
      geometry_3435 = st_transform(geometry, 3435),
      centroid = st_centroid(geometry_3435),
      area = st_area(geometry_3435)
    ) %>%
    cbind(
      st_coordinates(st_transform(.$centroid, 4326)),
      st_coordinates(.$centroid)
    ) %>%
    select(
      year,
      lon = X, lat = Y, x_3435 = X.1, y_3435 = Y.1,
      geometry, geometry_3435
    )
  geoparquet_to_s3(osm_df_clean, osm_remote)
}


##### Microsoft #####
ms_geojson <- here(footprint_tmp_dir, "ms-2019.geojson")
ms_remote_raw <- file.path(input_bucket, "microsoft", "2019.geojson")
ms_remote <- file.path(
  output_bucket, "source=microsoft", "microsoft-2019.parquet"
)

if (!aws.s3::object_exists(ms_remote)) {
  if (!file.exists(ms_geojson)) {
    aws.s3::save_object(ms_remote_raw, file = ms_geojson)
  }

  # The microsoft footprints file includes the whole state of IL
  # We need to clip it to only include Cook County
  cook_boundary <- read_geoparquet_sf(
    file.path(
      AWS_S3_WAREHOUSE_BUCKET,
      "spatial/ccao/county/2019.parquet"
    )
  ) %>%
    st_transform(4326)

  # Load raw geojson file and keep only valid geometries
  ms_df <- st_read(ms_geojson)
  ms_df_clean <- ms_df %>%
    st_transform(4326) %>%
    st_cast("MULTIPOLYGON") %>%
    filter(st_is_valid(geometry))

  # Limit to cook footprints only + add more geometry
  ms_df_clean_cook_only <- ms_df_clean %>%
    filter(
      as.logical(st_intersects(
        geometry,
        cook_boundary
      ))
    ) %>%
    mutate(
      year = 2019,
      geometry_3435 = st_transform(geometry, 3435),
      centroid = st_centroid(geometry_3435),
      area = st_area(geometry_3435)
    ) %>%
    cbind(
      st_coordinates(st_transform(.$centroid, 4326)),
      st_coordinates(.$centroid)
    ) %>%
    select(
      year,
      lon = X, lat = Y, x_3435 = X.1, y_3435 = Y.1,
      geometry, geometry_3435
    )
  geoparquet_to_s3(ms_df_clean_cook_only, ms_remote)
}
