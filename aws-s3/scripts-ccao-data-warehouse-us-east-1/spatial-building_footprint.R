library(arrow)
library(aws.s3)
library(DBI)
library(dplyr)
library(glue)
library(here)
library(noctua)
library(purrr)
library(sf)
library(sfarrow)
library(stringr)
library(tidyr)

# This script cleans building footprint data from various sources and
# compiles it into a single table on AWS Athena
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
footprint_tmp_dir <- here("footprint-tmp")
footprint_s3_dir <- file.path(
  AWS_S3_RAW_BUCKET, "spatial", "building_footprint"
)

##### ESRI #####
esri_chicago_geojson <- here(footprint_tmp_dir, "chicago-2008.geojson")
esri_sub_geojson <- here(footprint_tmp_dir, "suburban_cook-2008.geojson")
if (!file.exists(esri_chicago_geojson)) {
  aws.s3::save_object(
    file.path(footprint_s3_dir, "esri", "chicago-2008.geojson"),
    file = esri_chicago_geojson
  )
}
if (!file.exists(esri_sub_geojson)) {
  aws.s3::save_object(
    file.path(footprint_s3_dir, "esri", "suburban_cook-2008.geojson"),
    file = esri_sub_geojson
  )
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
    year, lon = X, lat = Y, x_3435 = X.1, y_3435 = Y.1,
    geometry, geometry_3435
  )
st_write_parquet(
  esri_chicago_df_clean,
  file.path(
    AWS_S3_WAREHOUSE_BUCKET, "spatial", "building_footprint",
    "source=esri", "chicago-2008.parquet"
  ),
  compression = "snappy"
)

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
    year, lon = X, lat = Y, x_3435 = X.1, y_3435 = Y.1,
    geometry, geometry_3435
  )
st_write_parquet(
  esri_sub_df_clean,
  file.path(
    AWS_S3_WAREHOUSE_BUCKET, "spatial", "building_footprint",
    "source=esri", "suburban_cook-2008.parquet"
  ),
  compression = "snappy"
)


##### OSM #####
osm_geojson <- here(footprint_tmp_dir, "osm-2021.geojson")
if (!file.exists(osm_geojson)) {
  aws.s3::save_object(
    file.path(footprint_s3_dir, "osm", "2021.geojson"),
    file = osm_geojson
  )
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
    year, lon = X, lat = Y, x_3435 = X.1, y_3435 = Y.1,
    geometry, geometry_3435
  )
st_write_parquet(
  osm_df_clean,
  file.path(
    AWS_S3_WAREHOUSE_BUCKET, "spatial", "building_footprint",
    "source=osm", "osm-2021.parquet"
  ),
  compression = "snappy"
)


##### Microsoft #####
ms_geojson <- here(footprint_tmp_dir, "ms-2019.geojson")
if (!file.exists(ms_geojson)) {
  aws.s3::save_object(
    file.path(footprint_s3_dir, "microsoft", "2019.geojson"),
    file = ms_geojson
  )
}

# The microsoft footprints file includes the whole state of IL
# We need to clip it to only include Cook County
cook_boundary <- st_read(
  paste0(
    "https://opendata.arcgis.com/datasets/",
    "ea127f9e96b74677892722069c984198_1.geojson"
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
    year, lon = X, lat = Y, x_3435 = X.1, y_3435 = Y.1,
    geometry, geometry_3435
  )
st_write_parquet(
  ms_df_clean_cook_only,
  file.path(
    AWS_S3_WAREHOUSE_BUCKET, "spatial", "building_footprint",
    "source=microsoft", "microsoft-2019.parquet"
  ),
  compression = "snappy"
)