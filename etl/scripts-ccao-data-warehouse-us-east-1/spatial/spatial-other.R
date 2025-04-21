library(aws.s3)
library(dplyr)
library(geoarrow)
library(purrr)
library(sf)
library(stringr)
source("utils.R")

AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")


##### SUBDIVISIONS #####
# Gather paths for subdivision shapefiles
subdivisions_raw <- grep(
  ".geojson",
  file.path(
    AWS_S3_RAW_BUCKET,
    aws.s3::get_bucket_df(
      AWS_S3_RAW_BUCKET,
      prefix = "spatial/other/subdivision/"
    )$Key
  ),
  value = TRUE
)

# Function to extract and transform geometry from shapefiles
walk(subdivisions_raw, function(shapefile_path) {
  dest_path <- file.path(
    AWS_S3_WAREHOUSE_BUCKET, "spatial", "other", "subdivision",
    paste0("year=", str_extract(shapefile_path, "[0-9]{4}")),
    "part-0.parquet"
  )

  if (!aws.s3::object_exists(dest_path)) {
    tmp_file <- tempfile(fileext = ".geojson")
    aws.s3::save_object(shapefile_path, file = tmp_file)

    # All we need is geometry column for this data, the other columns aren't
    # useful
    st_read(tmp_file) %>%
      rename(pagesubref = starts_with("PAGE")) %>%
      filter(st_is_valid(geometry) & !is.na(pagesubref)) %>%
      mutate(geometry_3435 = st_transform(geometry, 3435)) %>%
      select(pagesubref, geometry, geometry_3435) %>%
      geoparquet_to_s3(dest_path)

    file.remove(tmp_file)
  }
})


##### COMMUNITY AREAS ####
# Gather paths for community area shapefiles
comm_areas_raw <- grep(
  ".geojson",
  file.path(
    AWS_S3_RAW_BUCKET,
    aws.s3::get_bucket_df(
      AWS_S3_RAW_BUCKET,
      prefix = "spatial/other/community_area/"
    )$Key
  ),
  value = TRUE
)

# Function to extract and transform geometry from shapefiles
clean_comm_areas <- function(shapefile_path) {
  tmp_file <- tempfile(fileext = ".geojson")
  aws.s3::save_object(shapefile_path, file = tmp_file)

  # All we need is geometry column for this data, the other columns aren't
  # useful
  st_read(tmp_file) %>%
    st_transform(4326) %>%
    mutate(geometry_3435 = st_transform(geometry, 3435)) %>%
    select(
      community,
      area_number = area_numbe,
      geometry, geometry_3435
    ) %>%
    geoparquet_to_s3(
      file.path(
        AWS_S3_WAREHOUSE_BUCKET, "spatial", "other", "community_area",
        paste0("year=", str_extract(shapefile_path, "[0-9]{4}")),
        "part-0.parquet"
      )
    )
}

# Apply function
walk(comm_areas_raw, clean_comm_areas)
