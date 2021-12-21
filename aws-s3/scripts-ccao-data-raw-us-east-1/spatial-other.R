library(aws.s3)
library(dplyr)
library(purrr)
library(sf)
source("utils.R")

AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
output_bucket <- file.path(AWS_S3_RAW_BUCKET, "spatial", "other")
current_year <- strftime(Sys.Date(), "%Y")

sources_list <- bind_rows(list(
  # CHICAGO COMMUNITY AREA
  "cca_2018" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "cauq-8yn6?method=export&format=GeoJSON",
    "boundary" = "community_area",
    "year" = "2018"
  ),

  # UNINCORPORATED AREA
  "unc_2014" = c(
    "source" = "https://datacatalog.cookcountyil.gov/api/geospatial/",
    "api_url" = "kbr6-dyec?method=export&format=GeoJSON",
    "boundary" = "unincorporated_area",
    "year" = "2014"
  )
))

# Function to call referenced API, pull requested data, and write it to S3
pwalk(sources_list, function(...) {
  df <- tibble::tibble(...)
  open_data_to_s3(
    s3_bucket_uri = output_bucket,
    base_url = df$source,
    data_url = df$api_url,
    dir_name = df$boundary,
    file_year = df$year,
    file_ext = ".geojson"
  )
})


##### SUBDIVISIONS #####
remote_file_subdivision <- file.path(
  output_bucket, "subdivision",
  paste0(current_year, ".geojson")
)
tmp_file_subdivision <- "O:/CCAODATA/data/spatial/Subdivisions.geojson"
save_local_to_s3(remote_file_subdivision, tmp_file_subdivision)