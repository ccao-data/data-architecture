library(aws.s3)
library(dplyr)
library(sf)
library(osmdata)

# This script retrieves the spatial/location data for industrial corridors in
# the City of Chicago
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")

# List APIs from city site
api_info <- list(
  # INDUSTRIAL CORRIDORS
  "ind_2013" = c("source"   = "https://data.cityofchicago.org/api/geospatial/",
                 "api_url"  = "e6xh-nr8w?method=export&format=GeoJSON",
                 "boundary" = "industrial_corridor",
                 "year"     = "2013"),

  # PARKS
  "prk_2021" = c("source"   = "https://opendata.arcgis.com/datasets/",
                 "api_url"  = "52d4441a1e1743f58cd27408f564d11d_2.geojson",
                 "boundary" = "parks",
                 "year"     = "2021")
)


# Function to call referenced API, pull requested data, and write to S3
pull_and_write <- function(x) {

  remote_file <- file.path(
    AWS_S3_RAW_BUCKET, "spatial", "access",
    x["boundary"], paste0(x["year"], ".geojson"))

  if (!aws.s3::object_exists(remote_file)) {
    temp_file <- tempfile(fileext = ".geojson")
    st_read(paste0(x["source"], x["api_url"])) %>%
      st_write(temp_file)
    aws.s3::put_object(temp_file, remote_file)
    file.remove(temp_file)
  }
}

# Apply function to api_info
lapply(api_info, pull_and_write)

##### BUILDING FOOTPRINTS #####

remote_file <- file.path(
  AWS_S3_RAW_BUCKET, "spatial", "other",
  "building_footprint", paste0(strftime(Sys.Date(), "%Y"), ".geojson"))

if (!aws.s3::object_exists(remote_file)) {

  temp_file <- tempfile(fileext = ".geojson")

  # Gather building footprints from OSM
  footprints <- opq("Cook County United States") %>%
    add_osm_feature(key = "building") %>%
    osmdata_sf()

  # Only need polygons
  footprints <- footprints$osm_polygons

  # Transform
  footprints %>%
    st_transform(crs = 3435) %>%
    st_make_valid() %>%

    # Intersect with Cook County border
    st_intersection(
      st_transform(
        st_read(
          "https://opendata.arcgis.com/datasets/ea127f9e96b74677892722069c984198_1.geojson"
          ),
        3435
        )
      ) %>%
    pull(geometry) %>%

    # Upload to AWS
    st_write(temp_file)

  aws.s3::put_object(temp_file, remote_file, multipart = TRUE)
  file.remove(temp_file)

}

# Cleanup
rm(list = ls())