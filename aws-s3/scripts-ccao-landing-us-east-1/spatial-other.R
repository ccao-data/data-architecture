library(aws.s3)
library(dplyr)
library(sf)

AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
current_year <- strftime(Sys.Date(), "%Y")

# CHICAGO COMMUNITY AREA
remote_file_community_area <- file.path(
  AWS_S3_RAW_BUCKET, "spatial", "other", "community_area",
  paste0(current_year, ".zip")
)
tmp_file_community_area <- tempfile(fileext = ".geojson")

# Write file to S3 if it doesn't already exist
if (!aws.s3::object_exists(remote_file_community_area)) {
  st_read(paste0(
    "https://data.cityofchicago.org/api/geospatial/",
    "cauq-8yn6?method=export&format=GeoJSON"
  )) %>%
    st_write(tmp_file_community_area, delete_dsn = TRUE)
  aws.s3::put_object(tmp_file_community_area, remote_file_community_area)
  file.remove(tmp_file_community_area)
}

# UNINCORPORATED AREA
remote_file_unincorporated_area <- file.path(
  AWS_S3_RAW_BUCKET, "spatial", "other", "unincorporated_area",
  paste0("2014", ".zip")
)
tmp_file_unincorporated_area <- tempfile(fileext = ".geojson")

# Write file to S3 if it doesn't already exist
if (!aws.s3::object_exists(remote_file_unincorporated_area)) {
  st_read(paste0(
    "https://datacatalog.cookcountyil.gov/api/geospatial/",
    "kbr6-dyec?method=export&format=GeoJSON"
  )) %>%
    st_write(tmp_file_unincorporated_area, delete_dsn = TRUE)
  aws.s3::put_object(tmp_file_unincorporated_area, remote_file_unincorporated_area)
  file.remove(tmp_file_unincorporated_area)
}