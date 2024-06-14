library(aws.s3)
library(dplyr)
library(geoarrow)
library(osmdata)
library(sf)
source("utils.R")

# This script queries OpenStreetMap for grocery stores in Cook County and
# saves them as a spatial parquet
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, "spatial", "access")
current_year <- strftime(Sys.Date(), "%Y")

##### Grocery Stores #####

# Query OpenStreetMap API for grocery stores in Cook County
# Create a sequence of years from 2014 to the current year
years <- 2014:current_year

# Iterate over the years
for (year in years) {
  remote_file <- file.path(
    output_bucket, "grocery_store",
    paste0("year=", year),
    paste0("grocery_store-", year, ".parquet")
  )

  if (!aws.s3::object_exists(remote_file)) {
    # Update the datetime in the opq function
    supermarkets <- opq(
      bbox = "Cook County, IL",
      datetime = paste0(year, "-01-01T00:00:00Z"),
      timeout = 900
    ) %>%
      add_osm_features(features = c(
        "\"shop\"=\"supermarket\"",
        "\"shop\"=\"wholesale\"",
        "\"shop\"=\"greengrocer\""
      )) %>%
      osmdata_sf()

    supermarkets_polygons <- supermarkets$osm_polygons %>%
      select(osm_id, shop, name) %>%
      st_centroid()

    supermarkets_points <- supermarkets$osm_points %>%
      select(osm_id, shop, name) %>%
      st_centroid()

    rbind(supermarkets_polygons, supermarkets_points) %>%
      st_transform(4326) %>%
      filter(!is.na(name)) %>%
      mutate(
        geometry_3435 = st_transform(geometry, 3435)
      ) %>%
      select(osm_id, name, category = shop, geometry, geometry_3435) %>%
      geoarrow::write_geoparquet(remote_file)
  }
}
