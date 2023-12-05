# Script to generate shapefiles with indicators for corner lots.
#
# To run, set the township name variables below for the township you're
# interested in, then run the script. The script will output shapefiles to the
# `Base Data/` directory. These files should then be uploaded to the following
# S3 path:
#
# s3://ccao-data-raw-us-east-1/location/corner_lot/year={year}/*.shp
#
# At some point we may want to automate this upload, but it's manual for now.
#
# Note that the script can take upwards of 24 hours to complete, so you may
# want to run it as a Background Job in RStudio or via a tmux session on
# the server to ensure that your connection doesn't get killed.

library(osmdata)
library(tidyverse)
library(sf)
library(ggmap)
library(tidygraph)
library(sfnetworks)
library(dbscan)
library(units)
library(tmap)
library(rgrass)
library(link2GI)
library(nabor)
library(terra)
library(nngeo)
library(geosphere)
library(profvis)
library(furrr)

# We need two slightly different representations of the township name in order
# to satisfy the requirements of the different external systems we query.
# townshipa is used to filter our shapefiles, and should be the same as the
# township_name in ccao::town_dict
townshipa <- "Bloom"
# township is used to query Cook County open data, which sometimes has a slightly
# different format. Unfortunately there's no canonical crosswalk, so we
# typically just use trial and error, and most of the time township is
# equivalent to townshipa
township <- "Bloom"

bbox <- ccao::town_shp %>%
  filter(township_name == townshipa) %>%
  st_bbox()

bbox[1] <- bbox[1] -.0001
bbox[2] <- bbox[2] -.0001
bbox[3] <- bbox[3] +.0001
bbox[4] <- bbox[4] +.0001


# Download Street networks from Open Street Maps.
osm_data <- function(type) {
  opq(bbox = bbox) %>%
    add_osm_feature(key = "highway", value = type) %>%
    add_osm_feature(key = "highway", value = "!footway") %>%
    add_osm_feature(key = "highway:tag", value = "!alley") %>%
    osmdata_sf()
}

# Filter the street network to remove both major and minor roads.
# This simultaneously removes things like walking paths and highways, each
# of which would not be understood as typical "corners".
highway_type <- available_tags("highway")

town_osm <- lapply(highway_type, osm_data)

town_osm_center <- town_osm$Value$osm_lines %>%
  filter(
    !highway %in% c(
      "bridleway", "construction", "corridor", "cycleway", "elevator",
      "service", "services", "steps", "platform", "motorway", "motorway_link",
      "pedestrian", "track", "path"
    )
  )


# Construct the street network
network <- suppressWarnings({
  as_sfnetwork(town_osm_center, directed = FALSE) %>%
    activate(edges) %>%
    arrange(edge_length()) %>%
    filter(!edge_is_multiple()) %>%
    filter(!edge_is_loop()) %>%
    st_transform(3435) %>%
    convert(to_spatial_simple) %>%
    convert(to_spatial_subdivision) %>%
    activate(nodes) %>%
    mutate(degree = centrality_degree())
})


network_sf <- st_as_sf(network)

# Load parcel data from the County's data portal. When trial and erroring the township,
# you may need to modify this to include capital letters for "Town of".
parcels <- st_read(
  glue::glue(
    "https://datacatalog.cookcountyil.gov/resource/77tz-riq7.geojson?PoliticalTownship=Town%20of%20{township}&$limit=1000000"
  )) %>%
  mutate(id = row_number())

# Prepare inputs.
# Buffer each parcel so that we can leave room for error when searching
# for neighbors
parcels_buffered <- parcels %>%
  st_transform(3435) %>%
  st_buffer(dist = units::set_units(5, "m"))

network_trans  <- network %>%
  activate("edges") %>%
  activate("nodes") %>%
  filter(!node_is_isolated()) %>%
  activate("edges") %>%
  st_as_sf() %>%
  st_transform(3435)


# Function that takes a parcel index `x` and uses a dataframe of parcels
# (`parcels_full`) and a dataframe of buffered parcels (`parcels_buffered`)
# alongside a street network (`network`) to determine whether the parcel at
# the given index is a corner lot or not. Returns a boolean
 parcel_is_corner <- function(x, parcels_full, parcels_buffered, network) {

  stopifnot(
    st_crs(parcels_full) == st_crs(parcels_buffered),
    st_crs(parcels_buffered) == st_crs(network)
  )

  # Step 1: Create the minimum rectangle that bounds the parcel
  min_rectangle <- parcels_full[x, ] %>%
    st_minimum_rotated_rectangle() %>%
    select(geometry)

  min_rectangle_line <- st_segments(min_rectangle) %>%
    st_transform(crs = 4326)

  # Step 2: Draw a cross that cuts the parcel in four and extends out slightly
  # beyond the minimum rectangle of the parcel. We will look for parcels and
  # streets that intersect this cross in order to determine neighbors. The
  # first step in drawing this cross is determining the bearing of the parcel
  rectangle_network <- as_sfnetwork(min_rectangle_line, directed = FALSE) %>%
    activate(edges) %>%
    mutate(bearing = edge_azimuth()) %>%
    mutate(bearing = units::set_units(bearing, "degree")) %>%
    mutate(id = row_number()) %>%
    mutate(corrected_bearing = ifelse(id %% 4 != 0, bearing, bearing + units::set_units(180, "degree"))) %>%
    st_as_sf()


  # Calculate the lengths of the minimum rectangle edges
  rectangle_network <- rectangle_network %>%
    mutate(length = st_length(result) + units::set_units(10, "m"))

  # Calculate the centroid of the parcel, which will be the starting point of
  # the cross
  centroid <- suppressWarnings({
    st_centroid(parcels_full[x, ]) %>%
      st_as_sf() %>%
      st_transform(4326) %>%
      select(geometry)
  })

  centroid <- centroid %>%
    slice(rep(1:n(), each = 4)) %>%
    st_transform(crs = 4326) %>%
    st_coordinates()


  # Get the endpoints of the cross
  dest <- destPoint(p = centroid, b = rectangle_network$corrected_bearing, d = rectangle_network$length)

  cross <- cbind(centroid, dest) %>%
    as.data.frame()


  # Draw the lines that connect the centroids to the endpoints of the cross
  draw_line <- function(r) st_linestring(t(matrix(unlist(r), 2, 2)))
  cross$geometry <- st_sfc(sapply(1:nrow(cross), function(i) {
    draw_line(cross[i, ])
  }, simplify = FALSE))

  # Compute the length and aspect ratio of the cross. The aspect ratio will be
  # useful for filtering out parcels that are dramatically longer than wide
  cross <- cross %>%
    st_set_geometry("geometry") %>%
    st_set_crs(4326) %>%
    mutate(
      length = st_length(geometry),
      aspect_ratio = as.numeric(lag(length) / length),
      id = rep(1:(nrow(.) / 4), each = 4),
      branch = rep(1:4, length.out = n())
    ) %>%
    st_transform(3435)

  # Step 3: Find all of the neighboring units touched by the cross.
  # Start by computing the list of parcels that are intersected by the cross
  touching_unit <- suppressWarnings(suppressMessages(st_intersects(cross$geometry, parcels_full)))

  # Compute the neighbor units for a parcel
  neighbor_unit <-
    list(which(replace_na(as.logical(st_intersects(parcels_full, parcels_buffered[x, ])), FALSE)))
  neighbor_unit <- rep(neighbor_unit, each = 4)

  # Compute the intersection of {touching_unit & neighbor_unit} to find
  # neighboring units that intersect the cross
  neighbor_and_touching <- lapply(1:4, function(i) setdiff(intersect(touching_unit[[i]], neighbor_unit[[i]]), x))

  # Remove the cross segments from the intersection of
  # {cross_touching_unit & neighbor_unit}
  cross_int <- replace_na(imap_lgl(neighbor_and_touching, function(y, i) {
    as.logical(st_intersects(cross$geometry[i], parcels_full[i, ]))
  }), FALSE)

  cross_int <- map_lgl(neighbor_and_touching, function(x) length(x) > 0)

  cross_filter <- cross %>%
    mutate(is_neighboring = cross_int) %>%
    mutate(id = 1:4) %>%
    filter(!is_neighboring)

  # Step 4: Calculate the angle of cross segments in order to filter out
  # segments that are not at right angles. This is important because we don't
  # want parallel segments at opposite sides of the parcel, which would not
  # indicate a corner. Note that this approach doesn't work in all cases, so
  # we also introduce a second back_front_indicator below
  angle_diff <- function(theta1, theta2) {
    theta <- abs(theta1 - theta2) %% 360
    return(ifelse(theta > 180, 360 - theta, theta))
  }

  # Filter for only right-angle segments
  cross_corner <- rectangle_network %>%
    mutate(bearing = as.numeric(bearing)) %>%
    filter(id %in% cross_filter$id) %>%
    arrange(id) %>%
    mutate(
      diff_degree = angle_diff(bearing, lag(bearing)),
      diff_degree = replace_na(diff_degree, 0)
    ) %>%
    filter((diff_degree >= 85 & diff_degree <= 95), na.rm = TRUE) %>%
    st_set_geometry("result") %>%
    st_transform(3435)


  # Step 5: Get the number of roads that are bordering the parcel
  touching_unit_street <- rep(list(NULL), 4)

  touching_unit_street <- suppressWarnings(suppressMessages(
    st_intersects(cross_filter$geometry, network)
  ))

  touching_unit_street <- c(touching_unit_street, rep(list(NULL), 4 - length(touching_unit_street)))

  touching_street_number <- sum(lengths(touching_unit_street))

  cross_corner_number <- nrow(cross_corner)

  aspect_ratio <- max(cross$aspect_ratio, na.rm = TRUE)

  # Check for an edge case where two corner segments are actually on opposing
  # sides of the parcel
  back_front_indicator <- ifelse(
    touching_street_number == 2 &
      (!is_empty(touching_unit_street[[1]]) & !is_empty(touching_unit_street[[3]])) |
      (touching_street_number == 2 & !is_empty(touching_unit_street[[2]]) & !is_empty(touching_unit_street[[4]])),
    TRUE,
    FALSE
  )

  back_front_indicator <- as.numeric(back_front_indicator)

  # Check for an edge case where the same road has two representations in the
  # network, and so gets erroneously counted as two roads
  double_road_indicator <- ifelse(
    touching_street_number > 1 &
      (is_empty(touching_unit_street[[1]]) & is_empty(touching_unit_street[[2]]) & is_empty(touching_unit_street[[3]])) |
      (is_empty(touching_unit_street[[2]]) & is_empty(touching_unit_street[[3]]) & is_empty(touching_unit_street[[4]])) |
      (is_empty(touching_unit_street[[1]]) & is_empty(touching_unit_street[[3]]) & is_empty(touching_unit_street[[4]])) |
      (is_empty(touching_unit_street[[1]]) & is_empty(touching_unit_street[[2]]) & is_empty(touching_unit_street[[4]])),
      TRUE,
    FALSE
  )

  double_road_indicator <- as.numeric(double_road_indicator)

  # Step 6: Check if the parcel meets all the necessary conditions to be counted
  # as a corner
  if (all(touching_street_number >= 2) & all(cross_corner_number >= 1) & all(aspect_ratio < 30) & back_front_indicator == 0 & double_road_indicator == 0){
    return(TRUE)
  } else {
    return(FALSE)
  }
}

# Parcel data needs to be chunked in order to reduce memory use. Set the
# size of the chunks based on the capacity of the compute environment
chunk_size <- 5000

# Calculate the total number of chunks
num_chunks <- ceiling(nrow(parcels) / chunk_size)

# Create a list to store the results
results_list <- vector("list", length = num_chunks)

# Iterate through chunks of parcels and apply the corner indicator function
results_list <- map(1:num_chunks, function(chunk_number) {
  start_index <- (chunk_number - 1) * chunk_size + 1
  end_index <- min(chunk_number * chunk_size, nrow(parcels))

  parcel_slice <- parcels %>%
    slice(start_index:end_index) %>%
    st_transform(3435)

  result <- numeric(nrow(parcel_slice))

  for (x in 1:nrow(parcel_slice)) {
    result[x] <- parcel_is_corner(x, parcel_slice, parcels_buffered, network_trans)
    cat("Chunk:", chunk_number, "/", num_chunks, "- Iteration:", x, "/", nrow(parcel_slice), "\n")
  }

  return(result)
})

# Combine the results from each chunk
final_result <- unlist(results_list)

final <- cbind(parcels, final_result)

# Write the parcel results to disk
file_name <- paste0(township, "_11_20.shp")

directory <- file.path("Base Data", township)

output_file <- file.path(directory, file_name)

sf_obj <- final %>%
  select(pin10, final_result, geometry)

if (!dir.exists(directory)) {
  dir.create(directory, recursive = TRUE)
}

sf::st_write(sf_obj, output_file, append = FALSE)


# Write the street network to disk, to aid in debugging and visualization.
file_name <- paste0(township, "_street.shp")

output_file <- file.path(directory, file_name)

network_trans_export <- network_trans %>%
  select(name, osm_id, geometry, .tidygraph_edge_index)

st_write(network_trans_export, output_file, append = FALSE)
