library(arrow)
library(aws.s3)
library(dplyr)
library(geoarrow)
library(osmdata)
library(sf)
library(sfnetworks)
library(tidygraph)
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
# We only add new secondary roads if they were not previously major roads, are
# not in the corresponding year data in our additive major roads, and if they
# don't become major roads in the future. This is accomplished by filtering on
# osm_id and name, along with some spatial network cleaning to clean up what was
# missed. This is to prevent classifying a road as both major and secondary.

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
    paste0("secondary_road-", year, ".parquet")
  )

  # Simplify linestrings
  current_data <- read_geoparquet_sf(ingest_file_secondary) %>%
    mutate(geometry_3435 = st_simplify(geometry_3435, dTolerance = 10))

  # Initiate master data set with first available year, add column for de-duping
  if (is.null(master_dataset)) {
    master_dataset <- current_data %>%
      mutate(temporal = 0)

    data_to_write <- current_data
  } else {
    # Major roads data path from 1 year before and after
    ingest_file_major_prior <- file.path(
      AWS_S3_WAREHOUSE_BUCKET, "spatial",
      "environment", "major_road",
      paste0("year=", year - 1),
      paste0("major_road-", year - 1, ".parquet")
    )

    ingest_file_major_current <- file.path(
      AWS_S3_WAREHOUSE_BUCKET, "spatial",
      "environment", "major_road",
      paste0("year=", year),
      paste0("major_road-", year, ".parquet")
    )

    # Ingest Major roads data for the prior and current year
    major_roads_prior <- read_geoparquet_sf(ingest_file_major_prior)
    major_roads_current <- read_geoparquet_sf(ingest_file_major_current)

    # This if/else block prevents us from indexing a future
    # year that doesn't exist yet
    if (year < current_year) {
      ingest_file_major_post <- file.path(
        AWS_S3_WAREHOUSE_BUCKET, "spatial",
        "environment", "major_road",
        paste0("year=", year + 1),
        paste0("major_road-", year + 1, ".parquet")
      )

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
        filter(
          !osm_id %in% major_roads_prior$osm_id,
          !osm_id %in% major_roads_post$osm_id,
          !name %in% major_roads_current$name
        )
    } else {
      # Apply filter only for prior year major roads
      current_data <-
        current_data %>%
        filter(
          !osm_id %in% major_roads_prior$osm_id,
          !name %in% major_roads_current$name
        )
    }

    # Create temporal column to preserve earliest data
    combined_data <- bind_rows(
      master_dataset,
      current_data %>% mutate(temporal = 1)
    )

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

    # - - - - - - - - - - - - - -
    # Use networks to trim remaining islands
    # presumably resulting from OSM tag mismatches
    # - - - - - - - - - - - - - -
    dedup_network <- dedup_data %>% as_sfnetwork(directed = FALSE)

    # Identify connected components in the network
    network <- dedup_network %>%
      activate(nodes) %>%
      mutate(component = as.factor(group_components()))

    # Create a mapping of node components
    node_component <- as_tibble(network, active = "nodes")

    # Map component data to edges
    network <- network %>%
      activate(edges) %>%
      mutate(
        from_component = node_component$component[from],
        to_component = node_component$component[to]
      )

    # Here we assume that an edge belongs to a component if both its nodes do
    network <- network %>%
      mutate(edge_component = ifelse(from_component == to_component,
        from_component,
        NA
      ))

    # First convert the network with edge components into a regular tibble
    network_edges <- as_tibble(network, active = "edges")

    # Now calculate the size of each component
    component_sizes <- network_edges %>%
      filter(!is.na(edge_component)) %>%
      group_by(edge_component) %>%
      summarise(size = n())

    # Trim islands
    trimmed_data <- component_sizes %>%
      filter(size > 5) %>%
      mutate(geometry_3435 = st_transform(geometry, 3435)) %>%
      select(-edge_component, -size)

    # Union and intersect to restore original format
    data_to_write <- dedup_data %>%
      st_intersection(st_union(trimmed_data)) %>%
      mutate(geometry = st_transform(geometry_3435, 4326))
  }

  # Define the output file path for the data to write
  output_file <- file.path(
    AWS_S3_WAREHOUSE_BUCKET, "spatial",
    "environment", "secondary_road",
    paste0("year=", year),
    paste0("secondary_road-", year, ".parquet")
  )

  geoarrow::write_geoparquet(data_to_write, output_file)
}
