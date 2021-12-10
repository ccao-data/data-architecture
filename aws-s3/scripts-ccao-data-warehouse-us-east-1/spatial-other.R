library(aws.s3)
library(dplyr)
library(sf)

AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")

# UNINCORPORATED AREA
remote_file_unincorporated_area <- file.path(
  AWS_S3_RAW_BUCKET, "spatial", "other", "unincorporated_area",
  paste0("2014", ".zip")
)

# SUBDIVISIONS

# Gather paths for subdivision shapefiles
subdivisions_raw <- grep(
  ".geojson",
  file.path(
    AWS_S3_RAW_BUCKET,
    aws.s3::get_bucket_df(AWS_S3_RAW_BUCKET, prefix = 'spatial/other/subdivision/')$Key
  ),
  value = TRUE
)

# Function to extract and transform geometry from shapefiles
clean_subdivisions <- function(shapefile_path) {

  tmp_file <- tempfile(fileext = ".geojson")
  aws.s3::save_object(shapefile_path, file = tmp_file)

  # All we need is geometry column for this data, the other columns aren't useful
  st_read(tmp_file) %>%
    filter(st_is_valid(.)) %>%
    select(geometry) %>%
    st_transform(3435) %>%
    sfarrow::st_write_parquet(
      file.path(
        AWS_S3_WAREHOUSE_BUCKET, "spatial", "other", "subdivision",
        gsub("geojson", "parquet", basename(shapefile_path))
      )
    )

}

# Apply function
lapply(subdivisions_raw, clean_subdivisions)

# Cleanup
rm(list = ls())