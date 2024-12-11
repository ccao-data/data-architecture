library(aws.s3)
library(arrow)
library(dplyr)
library(geoarrow)
library(purrr)
library(rmapshaper)
library(sf)
library(tigris)
source("utils.R")

# This script cleans geometry files and moves them to a specific folder for use
# in Tableau and other visualization software
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, "export")

# Shapefile of the Cook County boundary for clipping
cook_boundary <- read_geoparquet_sf(
  file.path(
    AWS_S3_WAREHOUSE_BUCKET,
    "spatial/ccao/county/2019.parquet"
  )
) %>%
  st_transform(4326)

##### CENSUS TRACT #####
remote_file_tract_2022_warehouse <- file.path(
  AWS_S3_WAREHOUSE_BUCKET,
  "spatial", "census", "geography=tract",
  "year=2022", "tract-2022.parquet"
)
remote_file_tract_2022_export <- file.path(
  output_bucket, "geojson", "census-tract-2022.geojson"
)

if (!aws.s3::object_exists(remote_file_tract_2022_export)) {
  tracts_2022 <- read_geoparquet_sf(remote_file_tract_2022_warehouse) %>%
    filter(geoid != "17031990000") %>%
    select(geoid, geometry) %>%
    mutate(year = "2022") %>%
    st_transform(4326) %>%
    rmapshaper::ms_simplify(keep = 0.7, keep_shapes = TRUE)

  # Write geojson, then upload to S3
  tmp_file_geojson <- tempfile(fileext = ".geojson")
  st_write(tracts_2022, tmp_file_geojson)
  save_local_to_s3(remote_file_tract_2022_export, tmp_file_geojson)
}


##### CENSUS PUMA #####
remote_file_puma_2021_warehouse <- file.path(
  AWS_S3_WAREHOUSE_BUCKET,
  "spatial", "census", "geography=puma",
  "year=2021", "puma-2021.parquet"
)
remote_file_puma_2021_export <- file.path(
  output_bucket, "geojson", "census-puma-2021.geojson"
)
remote_file_ihs_warehouse <- file.path(
  AWS_S3_WAREHOUSE_BUCKET, "housing", "ihs_index",
  "year=2021", "part-0.parquet"
)

if (!aws.s3::object_exists(remote_file_puma_2021_export)) {
  ihs_index <- read_parquet(remote_file_ihs_warehouse)
  puma_2022 <- read_geoparquet_sf(remote_file_puma_2022_warehouse) %>%
    filter(geoid %in% c(ihs_index$geoid, "1703525")) %>%
    select(geoid, geometry) %>%
    mutate(year = "2021") %>%
    left_join(
      ihs_index %>%
        distinct(geoid, name),
      by = "geoid"
    ) %>%
    st_transform(4326) %>%
    st_intersection(cook_boundary) %>%
    rmapshaper::ms_simplify(keep = 0.7, keep_shapes = TRUE)

  # Write geojson, then upload to S3
  tmp_file_geojson <- tempfile(fileext = ".geojson")
  st_write(puma_2021, tmp_file_geojson)
  save_local_to_s3(remote_file_puma_2021_export, tmp_file_geojson)
}


##### CCAO TOWNSHIP #####
remote_file_town_warehouse <- file.path(
  AWS_S3_WAREHOUSE_BUCKET, "spatial", "ccao", "township", "2019.parquet"
)
remote_file_town_export <- file.path(
  output_bucket, "geojson", "ccao-township-2019.geojson"
)

if (!aws.s3::object_exists(remote_file_town_export)) {
  tmp_file_town <- tempfile(fileext = ".geojson")
  read_geoparquet_sf(remote_file_town_warehouse) %>%
    select(-geometry_3435) %>%
    st_transform(4326) %>%
    rmapshaper::ms_simplify(keep = 0.7, keep_shapes = TRUE) %>%
    st_write(tmp_file_town)

  save_local_to_s3(remote_file_town_export, tmp_file_town)
}


##### CCAO NEIGHBORHOOD #####
remote_file_nbhd_warehouse <- file.path(
  AWS_S3_WAREHOUSE_BUCKET, "spatial", "ccao",
  "neighborhood", "year=2021", "part-0.parquet"
)
remote_file_nbhd_export <- file.path(
  output_bucket, "geojson", "ccao-neighborhood-2021.geojson"
)

if (!aws.s3::object_exists(remote_file_nbhd_export)) {
  tmp_file_nbhd <- tempfile(fileext = ".geojson")
  nbhds <- read_geoparquet_sf(remote_file_nbhd_warehouse) %>%
    select(-geometry_3435) %>%
    st_transform(4326) %>%
    st_write(tmp_file_nbhd)

  save_local_to_s3(remote_file_nbhd_export, tmp_file_nbhd)
}


##### CHICAGO WARD #####
remote_file_ward_2023_warehouse <- file.path(
  AWS_S3_WAREHOUSE_BUCKET, "spatial", "political", "ward_chicago",
  "year=2023", "part-0.parquet"
)
remote_file_ward_2023_export <- file.path(
  output_bucket, "geojson", "chicago-ward-2023.geojson"
)

if (!aws.s3::object_exists(remote_file_ward_2023_export)) {
  tmp_file_ward_2023 <- tempfile(fileext = ".geojson")
  read_geoparquet_sf(remote_file_ward_2023_warehouse) %>%
    select(-geometry_3435) %>%
    st_transform(4326) %>%
    rmapshaper::ms_simplify(keep = 0.7, keep_shapes = TRUE) %>%
    st_write(tmp_file_ward_2023)

  save_local_to_s3(remote_file_ward_2023_export, tmp_file_ward_2023)
}


##### COOK MUNICIPALITY #####
remote_file_municipality_2022_warehouse <- file.path(
  AWS_S3_WAREHOUSE_BUCKET,
  "spatial", "political", "municipality",
  "year=2022", "part-0.parquet"
)
remote_file_municipality_2022_export <- file.path(
  output_bucket, "geojson", "cook-municipality-2022.geojson"
)

if (!aws.s3::object_exists(remote_file_municipality_2022_export)) {
  municipality_2022 <- read_geoparquet_sf(remote_file_municipality_2022_warehouse) %>%
    mutate(year = "2022") %>%
    st_transform(4326) %>%
    rmapshaper::ms_simplify(keep = 0.7, keep_shapes = TRUE)

  # Write geojson, then upload to S3
  tmp_file_geojson <- tempfile(fileext = ".geojson")
  st_write(municipality_2022, tmp_file_geojson)
  save_local_to_s3(remote_file_municipality_2022_export, tmp_file_geojson)
}


##### SCHOOL ELEMENTARY #####
remote_file_school_elem_2022_warehouse <- file.path(
  AWS_S3_WAREHOUSE_BUCKET,
  "spatial", "school", "school_district", "district_type=elementary",
  "year=2022", "part-0.parquet"
)
remote_file_school_elem_2022_export <- file.path(
  output_bucket, "geojson", "cook-school_elem-2022.geojson"
)

if (!aws.s3::object_exists(remote_file_school_elem_2022_export)) {
  school_elem_2022 <- read_geoparquet_sf(remote_file_school_elem_2022_warehouse) %>%
    select(geoid, name, school_num, is_attendance_boundary) %>%
    mutate(year = "2022") %>%
    st_transform(4326) %>%
    rmapshaper::ms_simplify(keep = 0.7, keep_shapes = TRUE)

  # Write geojson, then upload to S3
  tmp_file_geojson <- tempfile(fileext = ".geojson")
  st_write(school_elem_2022, tmp_file_geojson)
  save_local_to_s3(remote_file_school_elem_2022_export, tmp_file_geojson)
}


##### SCHOOL SECONDARY #####
remote_file_school_sec_2022_warehouse <- file.path(
  AWS_S3_WAREHOUSE_BUCKET,
  "spatial", "school", "school_district", "district_type=secondary",
  "year=2022", "part-0.parquet"
)
remote_file_school_sec_2022_export <- file.path(
  output_bucket, "geojson", "cook-school_sec-2022.geojson"
)

if (!aws.s3::object_exists(remote_file_school_sec_2022_export)) {
  school_sec_2022 <- read_geoparquet_sf(remote_file_school_sec_2022_warehouse) %>%
    select(geoid, name, school_num, is_attendance_boundary) %>%
    mutate(year = "2022") %>%
    st_transform(4326) %>%
    rmapshaper::ms_simplify(keep = 0.7, keep_shapes = TRUE)

  # Write geojson, then upload to S3
  tmp_file_geojson <- tempfile(fileext = ".geojson")
  st_write(school_sec_2022, tmp_file_geojson)
  save_local_to_s3(remote_file_school_sec_2022_export, tmp_file_geojson)
}


##### SCHOOL SECONDARY #####
remote_file_school_unif_2022_warehouse <- file.path(
  AWS_S3_WAREHOUSE_BUCKET,
  "spatial", "school", "school_district", "district_type=unified",
  "year=2022", "part-0.parquet"
)
remote_file_school_unif_2022_export <- file.path(
  output_bucket, "geojson", "cook-school_unif-2022.geojson"
)

if (!aws.s3::object_exists(remote_file_school_unif_2022_export)) {
  school_unif_2022 <- read_geoparquet_sf(remote_file_school_unif_2022_warehouse) %>%
    select(geoid, name, school_num, is_attendance_boundary) %>%
    mutate(year = "2022") %>%
    st_transform(4326) %>%
    rmapshaper::ms_simplify(keep = 0.7, keep_shapes = TRUE)

  # Write geojson, then upload to S3
  tmp_file_geojson <- tempfile(fileext = ".geojson")
  st_write(school_unif_2022, tmp_file_geojson)
  save_local_to_s3(remote_file_school_unif_2022_export, tmp_file_geojson)
}
