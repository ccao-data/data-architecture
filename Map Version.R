

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

townshipa_list <- c("Barrington", "Berwyn", "Bloom", "Bremen", "Calumet", "Cicero", 
                    "Elk Grove", "Evanston", "Hanover", "Hyde Park", "Jefferson", "Lake", 
                    "Lake View", "Lemont", "Leyden", "Lyons", "Maine", "New Trier", "Niles",
                    "North Chicago", "Northfield", "Norwood Park", "Oak Park", "Orland",
                    "Palatine", "Palos", "Proviso", "Rich", "River Forest", "Riverside", "Rogers Park", 
                    "Schaumburg", "South Chicago", "Stickney", "Thornton", "West Chicago",
                    "Wheeling", "Worth")

townshipa <- "Bloom"
township <- "Bloom"

bbox <- ccao::town_shp %>%
  filter(township_name == townshipa) %>%
  st_bbox()

bbox

bbox[1] <- bbox[1] -.0001
bbox[2] <- bbox[2] -.0001
bbox[3] <- bbox[3] +.0001
bbox[4] <- bbox[4] +.0001


# Street network data
osm_data <- function(type) {
  opq(bbox = bbox) %>%
    add_osm_feature(key = "highway", value = type) %>%
    add_osm_feature(key = "highway", value = "!footway") %>%
    add_osm_feature(key = "highway:tag", value = "!alley") %>%
    osmdata_sf()
}

highway_type <- available_tags("highway")

town_osm <- lapply(highway_type, osm_data)

town_osm_center <- town_osm$Value$osm_lines %>%
  filter(!highway %in% c("bridleway", "construction", "corridor", "cycleway", "elevator", "service", "services", "steps", "platform", "motorway", "motorway_link", "pedestrian", "track", "path"))


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


# Assuming 'network' is your sfnetwork
network_sf <- st_as_sf(network)


ggplot() +
  geom_sf(data = st_as_sf(town_osm_center), aes(col = highway))

# Parcel data
parcels <- st_read(
  glue::glue(
    "https://datacatalog.cookcountyil.gov/resource/77tz-riq7.geojson?PoliticalTownship=Town%20of%20{township}&$limit=1000000"
  )) %>%
  mutate(id = row_number())


parcels <- parcels %>%
  filter(pin10 == 3206301029)

# Prepare inputs
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




dan_func_single_obs <- function(x, parcels_full, parcels_buffered, network) {
  
  stopifnot(
    st_crs(parcels_full) == st_crs(parcels_buffered),
    st_crs(parcels_buffered) == st_crs(network)
  )
  
  # Step 1: Create the minimum rectangle
  min_rectangle <- parcels_full[x, ] %>%
    st_minimum_rotated_rectangle() %>%
    select(geometry)
  
  min_rectangle_line <- st_segments(min_rectangle) %>%
    st_transform(crs = 4326)
  
  
  # Step 2: Draw the cross
  ## Find out the bearing angle
  rectangle_network <- as_sfnetwork(min_rectangle_line, directed = FALSE) %>%
    activate(edges) %>%
    mutate(bearing = edge_azimuth()) %>%
    mutate(bearing = units::set_units(bearing, "degree")) %>%
    mutate(id = row_number()) %>%
    mutate(corrected_bearing = ifelse(id %% 4 != 0, bearing, bearing + units::set_units(180, "degree"))) %>%
    st_as_sf()
  
  
  ## Step 3 Find out the distance
  rectangle_network <- rectangle_network %>%
    mutate(length = st_length(result) + units::set_units(10, "m"))
  
  ## Step 4 Find out the starting point
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
  
  
  ## Step 5 Draw the cross
  dest <- destPoint(p = centroid, b = rectangle_network$corrected_bearing, d = rectangle_network$length)
  
  cross <- cbind(centroid, dest) %>%
    as.data.frame()
  
  
  ## Step 6 Draw Line
  draw_line <- function(r) st_linestring(t(matrix(unlist(r), 2, 2)))
  
  
  ## Step 7 Cross Function
  cross$geometry <- st_sfc(sapply(1:nrow(cross), function(i) {
    draw_line(cross[i, ])
  }, simplify = FALSE))
  
  cross <- cross %>%
    st_set_geometry("geometry") %>%
    st_set_crs(4326) %>%
    mutate(
      length = st_length(geometry),
      aspect_ratio = as.numeric(lag(length) / length),
      id = rep(1:(nrow(.) / 4), each = 4)
    ) %>%
    st_transform(3435)
  
  # Find out all the units touched by the cross
  touching_unit <- suppressWarnings(suppressMessages(st_intersects(cross$geometry, parcels_full)))
  
  # Step 3: Find out all the neighbor units for a parcel
  neighbor_unit <-
    list(which(replace_na(as.logical(st_intersects(parcels_full, parcels_buffered[x, ])), FALSE)))
  neighbor_unit <- rep(neighbor_unit, each = 4)
  
  # Step 4: Find out the intersection of {cross_touching_unit & neighbor_unit}
  neighbor_and_touching <- lapply(1:4, function(i) setdiff(intersect(touching_unit[[i]], neighbor_unit[[i]]), x))
  
  # Step 5: Remove the cross segments in the intersection of {cross_touching_unit & neighbor_unit}

  
  cross_int <- replace_na(imap_lgl(neighbor_and_touching, function(y, i) {
    as.logical(st_intersects(cross$geometry[i], parcels_full[i, ]))
  }), FALSE)
  
  cross_int <- map_lgl(neighbor_and_touching, function(x) length(x) > 0)
  
  cross_filter <- cross %>%
    mutate(is_neighboring = cross_int) %>%
    mutate(id = 1:4) %>%
    filter(!is_neighboring)
  
  # Step 6: Calculate the angle of cross segments
  angle_diff <- function(theta1, theta2) {
    theta <- abs(theta1 - theta2) %% 360
    return(ifelse(theta > 180, 360 - theta, theta))
  }
  
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
  
  
  
  
  touching_unit_street <- suppressWarnings(suppressMessages(
    st_intersects(cross_filter$geometry, network)
  ))
  
  touching_street_number <- sum(lengths(touching_unit_street))
  
  cross_corner_number <- nrow(cross_corner)
  
  aspect_ratio <- max(cross$aspect_ratio, na.rm = TRUE)
  
  
  
  if (all(touching_street_number >= 2) & all(cross_corner_number >= 1) & all(aspect_ratio < 30)) {
    return(TRUE)
  } else {
    return(FALSE)
  }
}



# Assuming your dataset is named 'parcels'
chunk_size <- 5000

# Calculate the total number of chunks
num_chunks <- ceiling(nrow(parcels) / chunk_size)

# Create a list to store the results
results_list <- vector("list", length = num_chunks)

# Iterate through chunks and apply the function
results_list <- map(1:num_chunks, function(chunk_number) {
  start_index <- (chunk_number - 1) * chunk_size + 1
  end_index <- min(chunk_number * chunk_size, nrow(parcels))
  
  parcel_slice <- parcels %>%
    slice(start_index:end_index) %>%
    st_transform(3435)
  
  result <- numeric(nrow(parcel_slice))
  
  for (x in 1:nrow(parcel_slice)) {
    result[x] <- dan_func_single_obs(x, parcel_slice, parcels_buffered, network_trans)
    cat("Chunk:", chunk_number, "/", num_chunks, "- Iteration:", x, "/", nrow(parcel_slice), "\n")
  }
  
  return(result)
})

# Combine the results from each chunk
final_result <- unlist(results_list)

final <- cbind(parcels, final_result)


file_name <- paste0(townshipa, "_11_20.shp")

directory <- file.path("Base Data", townshipa)

output_file <- file.path(directory, file_name)

sf_obj <- final %>%
  select(pin10, final_result, geometry)

if (!dir.exists(directory)) {
  dir.create(directory, recursive = TRUE)
}

sf::st_write(sf_obj, output_file, append = FALSE)


file_name <- paste0(townshipa, "_street.shp")

output_file <- file.path(directory, file_name)

network_trans_export <- network_trans %>%
  select(name, osm_id, geometry, .tidygraph_edge_index)

st_write(network_trans_export, output_file, append = FALSE)
