library(aws.s3)
library(dplyr)
library(glue)
library(noctua)
library(osmdata)
library(purrr)
library(sf)
library(sfarrow)
source("utils.R")

# This script queries OpenStreetMap for major roads in Cook County and
# saves them as a spatial parquet
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, "spatial", "environment")
current_year <- strftime(Sys.Date(), "%Y")

##### Major roads #####
# Query OpenStreetMap API for major roads in Cook
remote_file <- file.path(
  output_bucket, "major_road",
  paste0("year=", current_year),
  paste0("major_road-", current_year, ".parquet")
)

if (!aws.s3::object_exists(remote_file)) {
  osm_roads <- opq(bbox = "Cook County, IL") %>%
    add_osm_feature(
      key = "highway",
      value = c("motorway", "trunk", "primary")
    ) %>%
    osmdata_sf() %>%
    .$osm_lines %>%
    select(osm_id, name) %>%
    st_transform(4326) %>%
    mutate(geometry_3435 = st_transform(geometry, 3435))

  st_write_parquet(osm_roads, remote_file)
}