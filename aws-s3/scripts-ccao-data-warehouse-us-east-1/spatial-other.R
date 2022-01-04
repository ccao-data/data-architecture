library(aws.s3)
library(dplyr)
library(purrr)
library(sf)
library(stringr)
source("utils.R")

AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")

##### UNINCORPORATED AREA ####
unincorporated_area_raw <- grep(
  ".geojson",
  file.path(
    AWS_S3_RAW_BUCKET,
    aws.s3::get_bucket_df(
      AWS_S3_RAW_BUCKET,
      prefix = "spatial/other/unincorporated_area/"
    )$Key
  ),
  value = TRUE
)

# Function to extract and transform geometry from shapefiles
clean_unincorporated_area <- function(shapefile_path) {
  tmp_file <- tempfile(fileext = ".geojson")
  aws.s3::save_object(shapefile_path, file = tmp_file)

  # Just need to clean up column names and transform
  st_read(tmp_file) %>%
    filter(st_is_valid(.)) %>%
    select(agencydesc, zonenotes, zoneid, zonedesc, zoneordina, geometry) %>%
    rename(zoneordinance = zoneordina, agency_desc = agencydesc) %>%
    rename_with(~ gsub("zone", "zone_", .x)) %>%
    mutate(
      zone_notes = na_if(zone_notes, ""),
      geometry_3435 = st_transform(geometry, 3435)
    ) %>%
    sfarrow::st_write_parquet(
      file.path(
        AWS_S3_WAREHOUSE_BUCKET, "spatial", "other", "unincorporated_area",
        paste0("year=", str_extract(shapefile_path, "[0-9]{4}")),
        "part-0.parquet"
      )
    )
}

# Apply function
walk(unincorporated_area_raw, clean_unincorporated_area)


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
clean_subdivisions <- function(shapefile_path) {
  tmp_file <- tempfile(fileext = ".geojson")
  aws.s3::save_object(shapefile_path, file = tmp_file)

  # All we need is geometry column for this data, the other columns aren't useful
  st_read(tmp_file) %>%
    filter(st_is_valid(geometry) & !is.na(PAGE_SUBRE)) %>%
    mutate(geometry_3435 = st_transform(geometry, 3435)) %>%
    select(pagesubref = PAGE_SUBRE, geometry, geometry_3435) %>%
    sfarrow::st_write_parquet(
      file.path(
        AWS_S3_WAREHOUSE_BUCKET, "spatial", "other", "subdivision",
        paste0("year=", str_extract(shapefile_path, "[0-9]{4}")),
        "part-0.parquet"
      )
    )
}

# Apply function
walk(subdivisions_raw, clean_subdivisions)


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

  # All we need is geometry column for this data, the other columns aren't useful
  st_read(tmp_file) %>%
    st_transform(4326) %>%
    mutate(geometry_3435 = st_transform(geometry, 3435)) %>%
    select(
      community,
      area_number = area_numbe,
      geometry, geometry_3435
    ) %>%
    sfarrow::st_write_parquet(
      file.path(
        AWS_S3_WAREHOUSE_BUCKET, "spatial", "other", "community_area",
        paste0("year=", str_extract(shapefile_path, "[0-9]{4}")),
        "part-0.parquet"
      )
    )
}

# Apply function
walk(comm_areas_raw, clean_comm_areas)