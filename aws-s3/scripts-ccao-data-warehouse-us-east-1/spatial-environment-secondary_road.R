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
# st_difference - sf
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
    dedup_data <- combined_data %>%
      arrange(osm_id, temporal) %>%
      group_by(osm_id) %>%
      slice(1) %>%
      ungroup() %>%
      select(-temporal)

    # Reset temporal tag for the next iteration
    master_dataset <- dedup_data %>%
      mutate(temporal = 0)


    # This code is an imperfect solution to the changing osm_id problem. We
    # condense road names into singular roads if they overlap. If they don't
    # overlap, they remain as two distinct geometries with the same road name.
    # This allows us to filter on distance, it doesn't catch everything, but
    # does catch a large majority of floating geometries.

    processed_data <- dedup_data %>%
      # Group the data by the 'name' field
      group_by(name) %>%
      # Apply a function to each group (each unique name)
      group_map(~ {
        # '.x' is the subset of data for the current group
        df <- .x
        # Calculate intersections of geometries within each group
        interactions <- st_intersects(df$geometry)

        # Map over each row in the group's data frame
        map_df(seq_len(nrow(df)), function(i) {
          # If the current row does not intersect with any other,
          # return it as is. This check avoids self-intersection
          if (!any(interactions[[i]] != i)) {
            return(df[i, ])
          }
          # Identify indices of geometries that intersect with the current one,
          # excluding the current geometry itself
          connected <- unlist(interactions[i]) %>% setdiff(i)
          # If there are connected geometries, merge them
          if (length(connected) > 0) {
            # Merge the current geometry with all connected ones
            merged_geom <- st_union(df$geometry[c(i, connected)])
            # Return a tibble with the merged geometry and the group name.
            return(tibble(name = df$name[i], geometry = merged_geom))
          } else {
            # If no connected geometries, return the current geometry as is
            return(df[i, ])
          }
        })
      }) %>%
      # Combine all the processed data frames into one data frame
      bind_rows()
      mutate(length = st_length(geometry),
             geometry_3435 = st_transform(geometry, 3435)) %>%
      st_as_sf(sf_column_name = "geometry")  %>%
      filter(length > units::set_units(250, "m")) %>%
      select(-length)

  }

  # Define the output file path for the data to write
  output_file <- file.path(
    AWS_S3_WAREHOUSE_BUCKET, "spatial",
    "environment", "secondary_road",
    paste0("year=", year),
    paste0("secondary_road-", year, ".parquet")
  )

  #geoarrow::write_geoparquet(processed_data, output_file)

}







