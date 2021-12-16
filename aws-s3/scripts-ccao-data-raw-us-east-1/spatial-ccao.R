library(aws.s3)
library(dplyr)
library(sf)

# This script retrieves CCAO neighborhood boundaries
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")

api_info <- list(
  # NEIGHBORHOOD
  "neighborhood" = c(
    "url" = paste0(
      "https://gitlab.com/ccao-data-science---modeling/packages/ccao",
      "/-/raw/master/data-raw/nbhd_shp.geojson"
    ),
    "boundary" = "neighborhood",
    "year" = "2021"
  ),

  # TOWNSHIP
  "township" = c(
    "url" = paste0(
      "https://gitlab.com/ccao-data-science---modeling/packages/ccao",
      "/-/raw/master/data-raw/town_shp.geojson"
    ),
    "boundary" = "township",
    "year" = "2019"
  )
)

# Function to call referenced API, pull requested data, and write it to S3
pull_and_write <- function(x) {
  tmp_file <- tempfile(fileext = ".geojson")
  remote_file <- file.path(
    AWS_S3_RAW_BUCKET, "spatial", "ccao",
    x["boundary"], paste0(x["year"], ".geojson")
  )

  if (!aws.s3::object_exists(remote_file)) {
    st_read(x["url"]) %>%
      st_write(tmp_file, delete_dsn = TRUE)

    aws.s3::put_object(tmp_file, remote_file)
    file.remove(tmp_file)
  }
}

# Apply function to "api_info"
lapply(api_info, pull_and_write)

# Cleanup
rm(list = ls())
