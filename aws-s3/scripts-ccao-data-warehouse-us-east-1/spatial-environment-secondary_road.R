library(arrow)
library(aws.s3)
library(dplyr)
library(geoarrow)
library(osmdata)
library(sf)
source("utils.R")

# This script is designed to ingest spatial data on secondary roads for each
# year from 2014 to the present, simplify it for efficiency, and store a
# deduplicated, aggregated version of this data in a warehouse bucket.
#
# We take an additive approach here to ensure distance to these roads is
# consistent from earlier pin-level data. If there are new secondary roads in
# 2015 data, they will be added to existing 2014 secondary roads data, and that
# addition will become our 2015 secondary roads data. If there are identical
# osm_id observations between 2014 and 2015, we preserve the data from 2014.
#
# Note: We only add new secondary roads if they were NOT previously major roads,
# and if they don't become major roads in the future.
#
# This is to prevent classifying a road as both major and secondary

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
  ingest_file_secondary <- file.path(
    AWS_S3_RAW_BUCKET, "spatial",
    "environment", "secondary_road",
    paste0("year=", year),
    paste0("secondary_road-", year, ".parquet"))

  # Simplify linestrings
  current_data <- read_geoparquet_sf(ingest_file_secondary) %>%
    mutate(geometry_3435 = st_simplify(geometry_3435, dTolerance = 10))

  # Initiate master data set with first available year, add column for de-duping
  if (is.null(master_dataset)) {
    master_dataset <- current_data %>%
      mutate(temporal = 0)

    data_to_write <- current_data
  } else {
    # Major roads data path from 1 year before
    ingest_file_major_prior <- file.path(
      AWS_S3_WAREHOUSE_BUCKET, "spatial",
      "environment", "major_road",
      paste0("year=", year - 1),
      paste0("major_road-", year - 1, ".parquet"))

    # Ingest Major roads data for the prior year
    major_roads_prior <- read_geoparquet_sf(ingest_file_major_prior)

      # This if/else block prevents us from indexing a future
      # year that doesn't exist yet
      if (year < current_year) {
        ingest_file_major_post <- file.path(
          AWS_S3_WAREHOUSE_BUCKET, "spatial",
          "environment", "major_road",
          paste0("year=", year + 1),
          paste0("major_road-", year + 1, ".parquet"))

        # Ingest Major roads data for the next year
        major_roads_post <- read_geoparquet_sf(ingest_file_major_post)

        # Apply filter for both prior and post year major roads, this filter
        # accounts for the case where:
        # - A previously major road becomes secondary
        # - A secondary becomes major in the future
        #
        # This way we don't double count a road for both major and secondary
        current_data <-
          current_data %>%
          filter(!osm_id %in% major_roads_prior$osm_id,
                 !osm_id %in% major_roads_post$osm_id)
      } else {
        # Apply filter only for prior year major roads
        current_data <-
          current_data %>%
          filter(!osm_id %in% major_roads_prior$osm_id)
      }

    # Create temporal column to preserve earliest data
    combined_data <- bind_rows(master_dataset,
                               current_data %>% mutate(temporal = 1))

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
    "environment", "secondary_road",
    paste0("year=", year),
    paste0("secondary_road-", year, ".parquet")
  )

  #geoarrow::write_geoparquet(data_to_write, output_file)

}

# IN DEVELOPMENT

# Split the data into subsets based on the 'name' field
split_data <- split(data_to_write, data_to_write$name)

# Initialize an empty list to store the processed merged data
processed_data <- list()

# Begin a loop to process each group (road name) individually
for (name in names(split_data)) {
  # Extract the data frame for the current group
  df <- split_data[[name]]

  # Use st_intersects to determine which linestrings overlap or touch each other
  interactions <- st_intersects(df$geometry)

  # Initialize a vector to keep track of which rows (linestrings)
  # have been processed
  processed_indices <- rep(FALSE, nrow(df))

  # Begin a loop to go through each linestring in the current group
  for (i in seq_len(nrow(df))) {
    # Process the linestring only if it hasn't been processed yet
    if (!processed_indices[i]) {
      # Identify other linestrings that intersect or touch the current one
      connected <- unlist(interactions[i])
      # Remove the current linestring from the list of connected linestrings
      connected <- connected[connected != i]

      # If there are connected linestrings, merge them
      if (length(connected) > 0) {
        # Merge the current linestring with all connected ones
        merged_geom <- st_union(df$geometry[c(i, connected)])
        # Add the merged geometry to the processed data list
        processed_data[[length(processed_data) + 1]] <-
          data.frame(name = name,
                     geometry = merged_geom,
                     stringsAsFactors = FALSE)
        # Mark all involved linestrings as processed
        processed_indices[c(i, connected)] <- TRUE
      } else {
        # If there are no connected linestrings, add the
        # current linestring as is
        processed_data[[length(processed_data) + 1]] <- df[i, , drop = FALSE]
      }
    }
  }
}

processed_data <-
  dplyr::bind_rows(processed_data) %>%
  mutate(length = st_length(geometry)) %>%
  st_as_sf(sf_column_name = "geometry") %>%
  filter(!grepl("milwaukee", name, ignore.case = T))

processed_data_subset_length <-
  processed_data %>% filter(length > units::set_units(300, "m"))

