library(arrow)
library(aws.s3)
library(dplyr)
library(glue)
library(here)
library(osmdata)
library(sf)
source("utils.R")

# This script grabs OSM street data for Cook County, IL and saves it to S3. The
# OSM data is used to create corner lot indicators for parcels in the county.
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(AWS_S3_RAW_BUCKET, "spatial", "ccao", "corner")

# Get the parcel file years for which we should make corner lot indicators
parcel_path <- file.path(AWS_S3_WAREHOUSE_BUCKET, "spatial", "parcel")
parcel_years <- open_dataset(parcel_path) %>%
  distinct(year) %>%
  # Drop years before 2014, since OSM data for roads is spotty prior to that
  filter(year >= 2014) %>%
  collect() %>%
  pull(year)

# Grab the bounding box for Cook County, which is used to query OSM for streets
cook_boundary <- read_s3_geoparquet(
  file.path(
    AWS_S3_WAREHOUSE_BUCKET, "spatial/ccao/county/2019.parquet"
  )
) %>%
  select(-geometry_3435) %>%
  st_transform(3435) %>%
  st_as_sfc() %>%
  st_buffer(3000, endCapStyle = "FLAT", joinStyle = "MITRE") %>%
  st_transform(4326) %>%
  st_bbox()

# Iterate over the years
for (iter_year in parcel_years) {
  remote_file <- file.path(output_bucket, paste0(iter_year, ".geojson"))

  # Fetch the OSM street network for the county, removing any OSM way types
  # that are not main roads
  if (!aws.s3::object_exists(remote_file)) {
    tmp_file <- tempfile(fileext = ".geojson")

    print(paste("Fetching OSM streets for year:", iter_year))
    osm_streets <- opq(
      bbox = cook_boundary,
      datetime = glue("{iter_year}-01-01T00:00:00Z"),
      timeout = 900
    ) %>%
      add_osm_feature(key = "highway") %>%
      osmdata_sf() %>%
      .$osm_lines %>%
      filter(
        !highway %in% c(
          "bridleway", "construction", "corridor", "cycleway", "elevator",
          "service", "services", "steps", "platform", "motorway",
          "motorway_link", "pedestrian", "track", "path", "footway", "alley"
        )
      ) %>%
      st_transform(3435) %>%
      st_write(tmp_file)

    save_local_to_s3(remote_file, tmp_file)
    file.remove(tmp_file)
  } else {
    print(paste("OSM streets already exist for year:", iter_year))
  }
}
