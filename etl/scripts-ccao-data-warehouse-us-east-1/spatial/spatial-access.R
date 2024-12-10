library(arrow)
library(aws.s3)
library(dplyr)
library(geoarrow)
library(here)
library(osmdata)
library(purrr)
library(sf)
library(stringr)
library(tidyr)
source("utils.R")

# This script cleans shapefiles that represent desirable amenities, such as
# parks and hospitals
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
input_bucket <- file.path(AWS_S3_RAW_BUCKET, "spatial", "access")
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, "spatial", "access")
current_year <- strftime(Sys.Date(), "%Y")

##### BIKE TRAIL #####
remote_file_bike_raw <- file.path(
  input_bucket, "bike_trail", "2021.geojson"
)
remote_file_bike_warehouse <- file.path(
  output_bucket, "bike_trail", "year=2021", "part-0.parquet"
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
    geoarrow::write_geoparquet(remote_file_bike_warehouse)
}


##### CEMETERY #####
remote_file_ceme_raw <- file.path(
  input_bucket, "cemetery", "2021.geojson"
)
remote_file_ceme_warehouse <- file.path(
  output_bucket, "cemetery", "year=2021", "part-0.parquet"
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
    geoarrow::write_geoparquet(remote_file_ceme_warehouse)
}


##### HOSPITAL #####
remote_file_hosp_raw <- file.path(
  input_bucket, "hospital", "2021.geojson"
)
remote_file_hosp_warehouse <- file.path(
  output_bucket, "hospital", "year=2021", "part-0.parquet"
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
    geoarrow::write_geoparquet(remote_file_hosp_warehouse)
}


##### PARK #####
# Switched to using OSM parks because the county-provided parks file is
# very incomplete
remote_files_park_warehouse <- file.path(
  output_bucket,
  paste0(
    "park/year=",
    get_bucket_df(AWS_S3_RAW_BUCKET, prefix = "spatial/access/park") %>%
      pull(Key) %>%
      str_flatten() %>%
      str_extract_all("[0-9]{4}")
  ),
  "part-0.parquet"
)

walk(remote_files_park_warehouse, function(x) {
  if (!aws.s3::object_exists(x)) {
    parks <- opq("Cook County United States") %>%
      add_osm_feature(key = "leisure", value = "park") %>%
      osmdata_sf()

    cook_boundary <- st_read_parquet(
      file.path(
        AWS_S3_WAREHOUSE_BUCKET,
        "spatial/ccao/county/2019.parquet"
      )
    ) %>%
      st_transform(4326)

    parks_df <- bind_rows(parks$osm_polygons, parks$osm_multipolygons) %>%
      st_make_valid() %>%
      st_cast("MULTIPOLYGON") %>%
      st_transform(4326) %>%
      filter(st_is_valid(.)) %>%
      select(osm_id, name, geometry) %>%
      mutate(geometry_3435 = st_transform(geometry, 3435)) %>%
      filter(
        as.logical(st_intersects(
          geometry,
          cook_boundary
        ))
      )

    geoarrow::write_geoparquet(parks_df, x, compression = "snappy")
  }
})




##### INDUSTRIAL CORRIDOR #####
remote_file_indc_raw <- file.path(
  input_bucket, "industrial_corridor", "2013.geojson"
)
remote_file_indc_warehouse <- file.path(
  output_bucket, "industrial_corridor", "year=2013", "part-0.parquet"
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
      name, region,
      num = no, hud_qualif, acres,
      geometry, geometry_3435
    ) %>%
    geoarrow::write_geoparquet(remote_file_indc_warehouse)
}

##### WALKABILITY #####
# data dictionary located on page 7 at
# https://datahub.cmap.illinois.gov/dataset/aac0d840-77b4-4e88-8a26-7220ac6c588f/
# resource/7f0d890f-e678-46f8-9a6e-8d0b6ad04ae7/download/WalkabilityMethodology.pdf
remote_file_walk_raw <- file.path(
  input_bucket, "walkability", "2017.geojson"
)
remote_file_walk_warehouse <- file.path(
  output_bucket, "walkability", "year=2017", "part-0.parquet"
)

if (!aws.s3::object_exists(remote_file_walk_warehouse)) {
  tmp_file_walk <- tempfile(fileext = ".geojson")
  aws.s3::save_object(remote_file_walk_raw, file = tmp_file_walk)

  temp <- st_read(tmp_file_walk) %>%
    st_transform(4326) %>%
    rename_with(tolower) %>%
    rename_with(~ gsub("sc$|sco|scor|score", "_score", .x)) %>%
    rename_with(~"walk_num", contains("subzone")) %>%
    rename(walkability_rating = walkabilit, amenities_score = amenities, transitaccess = transitacc) %>%
    standardize_expand_geo() %>%
    select(-contains("shape")) %>%
    mutate(year = "2017") %>%
    geoarrow::write_geoparquet(remote_file_walk_warehouse)
}
