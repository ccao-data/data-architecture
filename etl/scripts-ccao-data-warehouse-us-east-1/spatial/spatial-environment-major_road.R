library(arrow)
library(aws.s3)
library(dplyr)
library(geoarrow)
library(osmdata)
library(sf)
source("utils.R")

# This script is designed to ingest spatial data on major roads for each year
# from 2014 to the present, simplify it for efficiency, and store a
# deduplicated, aggregated version of this data in a warehouse bucket.
#
# We take an additive approach here to ensure distance to these roads is
# consistent from earlier pin-level data. If there are new major roads in 2015
# data, they will be added to existing 2014 major roads data, and that addition
# will become our 2015 major roads data. If there are identical osm_id
# observations between 2014 and 2015, we preserve the data from 2014.

# Instantiate S3 bucket names
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")

# Set up variables for iteration through years
current_year <- as.integer(strftime(Sys.Date(), "%Y"))
years <- 2014:current_year
master_dataset <- NULL

# Iterate over the years
for (year in years) {
  # Ingest path
  ingest_file <- file.path(
    AWS_S3_RAW_BUCKET, "spatial",
    "environment", "major_road",
    paste0("year=", year),
    paste0("major_road-", year, ".parquet")
  )

  # Simplify linestrings
  current_data <- read_geoparquet_sf(ingest_file) %>%
    mutate(geometry_3435 = st_simplify(geometry_3435, dTolerance = 10))

  # Initiate master data set with first available year, add column for de-duping
  if (is.null(master_dataset)) {
    master_dataset <- current_data %>%
      mutate(temporal = 0)

    data_to_write <- current_data
  } else {
    # Create temporal column to preserve earliest data
    combined_data <- bind_rows(
      master_dataset,
      current_data %>% mutate(temporal = 1)
    )

    # Arrange by osm_id and temporal, then deduplicate and preserve earlier data
    data_to_write <- combined_data %>%
      arrange(osm_id, temporal) %>%
      group_by(osm_id) %>%
      slice(1) %>%
      ungroup() %>%
      select(-temporal)

    # Reset temporal tag for the next iteration
    master_dataset <- data_to_write %>%
      mutate(temporal = 0)
  }

  # Define the output file path for the data to write
  output_file <- file.path(
    AWS_S3_WAREHOUSE_BUCKET, "spatial",
    "environment", "major_road",
    paste0("year=", year),
    paste0("major_road-", year, ".parquet")
  )

  geoparquet_to_s3(data_to_write, output_file)
}
