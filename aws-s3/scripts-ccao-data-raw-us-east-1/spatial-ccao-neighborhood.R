library(aws.s3)
library(dplyr)
library(sf)

# This script retrieves CCAO neighborhood boundaries
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")

api_info <- list(
  # NEIGHBORHOOD
  "nbd_2015" = c("source"   = "https://datacatalog.cookcountyil.gov/api/geospatial/",
                 "api_url"  = "wyzt-dzf8?method=export&format=GeoJSON",
                 "boundary" = "neighborhood",
                 "year"     = "2021"))

# Function to call referenced API, pull requested data, and write it to S3
pull_and_write <- function(x) {

  tmp_file <- tempfile(fileext = ".geojson")
  remote_file <- file.path(
    AWS_S3_RAW_BUCKET, "spatial", "ccao",
    x["boundary"], paste0(x["year"], ".geojson")
  )

  if (!aws.s3::object_exists(remote_file)) {

    st_read(paste0(x["source"], x["api_url"])) %>%
      st_write(tmp_file, delete_dsn = TRUE)

    aws.s3::put_object(tmp_file, remote_file)
    file.remove(tmp_file)
  }
}

# Apply function to "api_info"
lapply(api_info, pull_and_write)

# Cleanup
rm(list = ls())