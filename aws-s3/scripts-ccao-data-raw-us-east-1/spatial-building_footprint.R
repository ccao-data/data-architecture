library(aws.s3)
library(dplyr)
library(osmdata)
library(sf)

# Script to gather various sources of building footprint data
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
current_year <- strftime(Sys.Date(), "%Y")


# OSM BUILDING FOOTPRINTS
remote_file_osm <- file.path(
  AWS_S3_RAW_BUCKET, "spatial", "building_footprint",
  "osm", paste0(current_year, ".geojson"))

if (!aws.s3::object_exists(remote_file_osm)) {

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

  aws.s3::put_object(temp_file, remote_file_osm, multipart = TRUE)
  file.remove(temp_file)

}
