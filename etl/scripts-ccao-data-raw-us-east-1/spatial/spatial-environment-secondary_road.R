library(aws.s3)
library(dplyr)
library(geoarrow)
library(osmdata)
library(sf)
source("utils.R")

# This script queries OpenStreetMap for secondary roads in Cook County and
# saves them as a spatial parquet
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
output_bucket <- file.path(AWS_S3_RAW_BUCKET, "spatial", "environment")
current_year <- strftime(Sys.Date(), "%Y")

##### Secondary roads #####
# Query OpenStreetMap API for secondary roads in Cook
# Create a sequence of years from 2014 to the current year
years <- 2014:current_year

# Iterate over the years
for (year in years) {
  remote_file <- file.path(
    output_bucket, "secondary_road",
    paste0("year=", year),
    paste0("secondary_road-", year, ".parquet")
  )

  if (!aws.s3::object_exists(remote_file)) {
    # Update the datetime in the opq function
    osm_roads <- opq(
      bbox = "Cook County, IL",
      datetime = paste0(year, "-01-01T00:00:00Z"),
      timeout = 900
    ) %>%
      add_osm_feature(
        key = "highway",
        value = "secondary"
      ) %>%
      osmdata_sf() %>%
      .$osm_lines %>%
      select(osm_id, name) %>%
      st_transform(4326) %>%
      mutate(geometry_3435 = st_transform(geometry, 3435))

    geoparquet_to_s3(osm_roads, remote_file)
  }
}
