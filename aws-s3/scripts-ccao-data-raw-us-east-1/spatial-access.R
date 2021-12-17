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
  "ind_2013" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "e6xh-nr8w?method=export&format=GeoJSON",
    "boundary" = "industrial_corridor",
    "year" = "2013"
  ),

  # PARKS
  "prk_2021" = c(
    "source" = "https://opendata.arcgis.com/datasets/",
    "api_url" = "52d4441a1e1743f58cd27408f564d11d_2.geojson",
    "boundary" = "park",
    "year" = "2021"
  )
)


# Function to call referenced API, pull requested data, and write to S3
pull_and_write <- function(x) {
  remote_file <- file.path(
    AWS_S3_RAW_BUCKET, "spatial", "access",
    x["boundary"], paste0(x["year"], ".geojson")
  )

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
