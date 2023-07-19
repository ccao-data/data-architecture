

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
# library(terra)
# library(raster)

# Jefferson Lake Norwood South West Worth

# parcelsfull <- parcels



townshipa <- "West Chicago"
township <- "West"

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
  filter(!highway %in% c("bridleway", "construction", "corridor", "cycleway", "elevator", "service", "services", "steps"))


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



# Parcel data
parcels <- st_read(
  glue::glue(
    "https://datacatalog.cookcountyil.gov/resource/77tz-riq7.geojson?PoliticalTownship=Town%20of%20{township}&$limit=1000000"
  )) %>%
  mutate(id = row_number())



parcels <- parcels %>%
  slice(1:5000)



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
  
  # 
  # 
  # centroid_transformed <- st_transform(centroid, crs = 3435)
  # buffer_distance <- 500
  # bbox_buffer <- st_buffer(centroid_transformed, dist = buffer_distance)
  # clipped_network <- suppressWarnings(suppressMessages(st_crop(network, bbox_buffer)))
  # 
  # 
  
  
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
  # cross_int <- suppressMessages(imap_lgl(neighbor_and_touching, function(x, i) {
  
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


# dan_func_single_obs(9, parcels %>% st_transform(3435), parcels_buffered, network_trans)

# 
result <- numeric(nrow(parcels))
# 
for (x in 1:nrow(parcels)) {
  result[x] <- dan_func_single_obs(x, parcels %>% st_transform(3435), parcels_buffered, network_trans)
  cat("Iteration:", x, "/", nrow(parcels), "\n")  # Print iteration progress
}




final1 <- cbind(parcels, result)


ggplot() +
  geom_sf(data = final1, aes(fill = result)) +
  geom_sf(data = st_as_sf(network, 'edges'), col = 'green')








# Define the township and file name
file_name <- paste0(township, "1.shp")

# Specify the directory path
directory <- "Base Data"

# Create the complete file path
output_file <- file.path(directory, file_name)

# Create an sf object with your data
sf_obj <- final1  # Replace `your_sf_object` with your actual sf object

sf_obj <- sf_obj %>%
  select(pin10, result, geometry)

# Save the complete shapefile
sf::st_write(sf_obj, output_file, append = FALSE)












# Parcel data
parcels <- st_read(
  glue::glue(
    "https://datacatalog.cookcountyil.gov/resource/77tz-riq7.geojson?PoliticalTownship=Town%20of%20{township}&$limit=1000000"
  )) %>%
  mutate(id = row_number())



parcels <- parcels %>%
  slice(5001:10000)





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


# dan_func_single_obs(9, parcels %>% st_transform(3435), parcels_buffered, network_trans)

# 
result <- numeric(nrow(parcels))
# 
for (x in 1:nrow(parcels)) {
  result[x] <- dan_func_single_obs(x, parcels %>% st_transform(3435), parcels_buffered, network_trans)
  cat("Iteration:", x, "/", nrow(parcels), "\n")  # Print iteration progress
}



final2 <- cbind(parcels, result)



ggplot() +
  geom_sf(data = parcels) +
  geom_sf(data = st_as_sf(network, 'edges'), col = 'green')








# Define the township and file name
file_name <- paste0(township, "2.shp")

# Specify the directory path
directory <- "Base Data"

# Create the complete file path
output_file <- file.path(directory, file_name)

# Create an sf object with your data
sf_obj <- final2  # Replace `your_sf_object` with your actual sf object

sf_obj <- sf_obj %>%
  select(pin10, result, geometry)

# Save the complete shapefile
sf::st_write(sf_obj, output_file, append = FALSE)




 
 
 
 
 
 











 
 # Parcel data
 parcels <- st_read(
   glue::glue(
     "https://datacatalog.cookcountyil.gov/resource/77tz-riq7.geojson?PoliticalTownship=Town%20of%20{township}&$limit=1000000"
   )) %>%
   mutate(id = row_number())
 
 
 
 parcels <- parcels %>%
   slice(10001:15000)
 
 
 
 
 
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
 
 
 # dan_func_single_obs(9, parcels %>% st_transform(3435), parcels_buffered, network_trans)
 
 # 
 result <- numeric(nrow(parcels))
 # 
 for (x in 1:nrow(parcels)) {
   result[x] <- dan_func_single_obs(x, parcels %>% st_transform(3435), parcels_buffered, network_trans)
   cat("Iteration:", x, "/", nrow(parcels), "\n")  # Print iteration progress
 }
 
 
 
 final3 <- cbind(parcels, result)
 
 
 
 ggplot() +
   geom_sf(data = parcels) +
   geom_sf(data = st_as_sf(network, 'edges'), col = 'green')
 
 
 
 
 
 
 
 
 # Define the township and file name
 file_name <- paste0(township, "3.shp")
 
 # Specify the directory path
 directory <- "Base Data"
 
 # Create the complete file path
 output_file <- file.path(directory, file_name)
 
 # Create an sf object with your data
 sf_obj <- final3  # Replace `your_sf_object` with your actual sf object
 
 sf_obj <- sf_obj %>%
   select(pin10, result, geometry)
 
 # Save the complete shapefile
 sf::st_write(sf_obj, output_file, append = FALSE)
 
 
 
 
 
 
 
 
 # Parcel data
 parcels <- st_read(
   glue::glue(
     "https://datacatalog.cookcountyil.gov/resource/77tz-riq7.geojson?PoliticalTownship=Town%20of%20{township}&$limit=1000000"
   )) %>%
   mutate(id = row_number())
 
 
 
 parcels <- parcels %>%
   slice(15001:20000)
 
 
 
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
 
 
 # dan_func_single_obs(9, parcels %>% st_transform(3435), parcels_buffered, network_trans)
 
 # 
 result <- numeric(nrow(parcels))
 # 
 for (x in 1:nrow(parcels)) {
   result[x] <- dan_func_single_obs(x, parcels %>% st_transform(3435), parcels_buffered, network_trans)
   cat("Iteration:", x, "/", nrow(parcels), "\n")  # Print iteration progress
 }
 
 
 
 final4 <- cbind(parcels, result)
 
 

 
 
 # Define the township and file name
 file_name <- paste0(township, "4.shp")
 
 # Specify the directory path
 directory <- "Base Data"
 
 # Create the complete file path
 output_file <- file.path(directory, file_name)
 
 # Create an sf object with your data
 sf_obj <- final4  # Replace `your_sf_object` with your actual sf object
 
 sf_obj <- sf_obj %>%
   select(pin10, result, geometry)
 
 # Save the complete shapefile
 sf::st_write(sf_obj, output_file, append = FALSE)
 
 
 
 
 
 
 
 
 
 
 # Parcel data
 parcels <- st_read(
   glue::glue(
     "https://datacatalog.cookcountyil.gov/resource/77tz-riq7.geojson?PoliticalTownship=Town%20of%20{township}&$limit=1000000"
   )) %>%
   mutate(id = row_number())
 
 
 
 parcels <- parcels %>%
   slice(20001:25000)
 
 
 
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
 
 
 # dan_func_single_obs(9, parcels %>% st_transform(3435), parcels_buffered, network_trans)
 
 # 
 result <- numeric(nrow(parcels))
 # 
 for (x in 1:nrow(parcels)) {
   result[x] <- dan_func_single_obs(x, parcels %>% st_transform(3435), parcels_buffered, network_trans)
   cat("Iteration:", x, "/", nrow(parcels), "\n")  # Print iteration progress
 }
 
 
 
 final5 <- cbind(parcels, result)
 
 
 
 ggplot() +
   geom_sf(data = parcels) +
   geom_sf(data = st_as_sf(network, 'edges'), col = 'green')
 
 
 
 
 # Define the township and file name
 file_name <- paste0(township, "5.shp")
 
 # Specify the directory path
 directory <- "Base Data"
 
 # Create the complete file path
 output_file <- file.path(directory, file_name)
 
 # Create an sf object with your data
 sf_obj <- final5  # Replace `your_sf_object` with your actual sf object
 
 sf_obj <- sf_obj %>%
   select(pin10, result, geometry)
 
 # Save the complete shapefile
 sf::st_write(sf_obj, output_file, append = FALSE)
 
 
 
 
 
 
 
 # Parcel data
 parcels <- st_read(
   glue::glue(
     "https://datacatalog.cookcountyil.gov/resource/77tz-riq7.geojson?PoliticalTownship=Town%20of%20{township}&$limit=1000000"
   )) %>%
   mutate(id = row_number())
 
 
 
 parcels <- parcels %>%
   slice(25001:30000)
 
 
 
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
 
 
 # dan_func_single_obs(9, parcels %>% st_transform(3435), parcels_buffered, network_trans)
 
 # 
 result <- numeric(nrow(parcels))
 # 
 for (x in 1:nrow(parcels)) {
   result[x] <- dan_func_single_obs(x, parcels %>% st_transform(3435), parcels_buffered, network_trans)
   cat("Iteration:", x, "/", nrow(parcels), "\n")  # Print iteration progress
 }
 
 
 
 final6 <- cbind(parcels, result)
 
 
 
 ggplot() +
   geom_sf(data = parcels) +
   geom_sf(data = st_as_sf(network, 'edges'), col = 'green')
 
 
 
 
 
 
 
 
 # Define the township and file name
 file_name <- paste0(township, "6.shp")
 
 # Specify the directory path
 directory <- "Base Data"
 
 # Create the complete file path
 output_file <- file.path(directory, file_name)
 
 # Create an sf object with your data
 sf_obj <- final6  # Replace `your_sf_object` with your actual sf object
 
 sf_obj <- sf_obj %>%
   select(pin10, result, geometry)
 
 # Save the complete shapefile
 sf::st_write(sf_obj, output_file, append = FALSE)
 
 
 
 
 
 
 
 
 
 
 # Parcel data
 parcels <- st_read(
   glue::glue(
     "https://datacatalog.cookcountyil.gov/resource/77tz-riq7.geojson?PoliticalTownship=Town%20of%20{township}&$limit=1000000"
   )) %>%
   mutate(id = row_number())
 
 
 
 parcels <- parcels %>%
   slice(30001:35000)
 
 
 
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
 
 
 # dan_func_single_obs(9, parcels %>% st_transform(3435), parcels_buffered, network_trans)
 
 # 
 result <- numeric(nrow(parcels))
 # 
 for (x in 1:nrow(parcels)) {
   result[x] <- dan_func_single_obs(x, parcels %>% st_transform(3435), parcels_buffered, network_trans)
   cat("Iteration:", x, "/", nrow(parcels), "\n")  # Print iteration progress
 }
 
 
 
 final7 <- cbind(parcels, result)
 
 

 
 
 
 
 
 # Define the township and file name
 file_name <- paste0(township, "7.shp")
 
 # Specify the directory path
 directory <- "Base Data"
 
 # Create the complete file path
 output_file <- file.path(directory, file_name)
 
 # Create an sf object with your data
 sf_obj <- final7  # Replace `your_sf_object` with your actual sf object
 
 sf_obj <- sf_obj %>%
   select(pin10, result, geometry)
 
 # Save the complete shapefile
 sf::st_write(sf_obj, output_file, append = FALSE)
 
 
 
 
 
 
 # Parcel data
 parcels <- st_read(
   glue::glue(
     "https://datacatalog.cookcountyil.gov/resource/77tz-riq7.geojson?PoliticalTownship=Town%20of%20{township}&$limit=1000000"
   )) %>%
   mutate(id = row_number())
 
 
 
 parcels <- parcels %>%
   slice(35001:40000)
 
 
 
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
 
 
 # dan_func_single_obs(9, parcels %>% st_transform(3435), parcels_buffered, network_trans)
 
 # 
 result <- numeric(nrow(parcels))
 # 
 for (x in 1:nrow(parcels)) {
   result[x] <- dan_func_single_obs(x, parcels %>% st_transform(3435), parcels_buffered, network_trans)
   cat("Iteration:", x, "/", nrow(parcels), "\n")  # Print iteration progress
 }
 
 
 
 final8 <- cbind(parcels, result)
 
 
 
 ggplot() +
   geom_sf(data = parcels) +
   geom_sf(data = st_as_sf(network, 'edges'), col = 'green')
 
 
 
 
 
 
 
 
 # Define the township and file name
 file_name <- paste0(township, "8.shp")
 
 # Specify the directory path
 directory <- "Base Data"
 
 # Create the complete file path
 output_file <- file.path(directory, file_name)
 
 # Create an sf object with your data
 sf_obj <- final8  # Replace `your_sf_object` with your actual sf object
 
 sf_obj <- sf_obj %>%
   select(pin10, result, geometry)
 
 # Save the complete shapefile
 sf::st_write(sf_obj, output_file, append = FALSE)
 
 
 
 
 
 
 
 
 
 
 # Parcel data
 parcels <- st_read(
   glue::glue(
     "https://datacatalog.cookcountyil.gov/resource/77tz-riq7.geojson?PoliticalTownship=Town%20of%20{township}&$limit=1000000"
   )) %>%
   mutate(id = row_number())
 
 
 
 parcels <- parcels %>%
   slice(40001:45000)
 
 
 
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
 
 
 # dan_func_single_obs(9, parcels %>% st_transform(3435), parcels_buffered, network_trans)
 
 # 
 result <- numeric(nrow(parcels))
 # 
 for (x in 1:nrow(parcels)) {
   result[x] <- dan_func_single_obs(x, parcels %>% st_transform(3435), parcels_buffered, network_trans)
   cat("Iteration:", x, "/", nrow(parcels), "\n")  # Print iteration progress
 }
 
 
 
 final9 <- cbind(parcels, result)
 
 
 
 ggplot() +
   geom_sf(data = parcels) +
   geom_sf(data = st_as_sf(network, 'edges'), col = 'green')
 
 
 
 
 
 
 
 
 # Define the township and file name
 file_name <- paste0(township, "9.shp")
 
 # Specify the directory path
 directory <- "Base Data"
 
 # Create the complete file path
 output_file <- file.path(directory, file_name)
 
 # Create an sf object with your data
 sf_obj <- final9  # Replace `your_sf_object` with your actual sf object
 
 sf_obj <- sf_obj %>%
   select(pin10, result, geometry)
 
 # Save the complete shapefile
 sf::st_write(sf_obj, output_file, append = FALSE)
 
 
 
 
 
 
 
 
 
 
 # Parcel data
 parcels <- st_read(
   glue::glue(
     "https://datacatalog.cookcountyil.gov/resource/77tz-riq7.geojson?PoliticalTownship=Town%20of%20{township}&$limit=1000000"
   )) %>%
   mutate(id = row_number())
 
 
 
 parcels <- parcels %>%
   slice(45001:50000)
 
 
 
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
 
 
 # dan_func_single_obs(9, parcels %>% st_transform(3435), parcels_buffered, network_trans)
 
 # 
 result <- numeric(nrow(parcels))
 # 
 for (x in 1:nrow(parcels)) {
   result[x] <- dan_func_single_obs(x, parcels %>% st_transform(3435), parcels_buffered, network_trans)
   cat("Iteration:", x, "/", nrow(parcels), "\n")  # Print iteration progress
 }
 
 
 
 final10 <- cbind(parcels, result)
 
 
 
 ggplot() +
   geom_sf(data = parcels) +
   geom_sf(data = st_as_sf(network, 'edges'), col = 'green')
 
 
 
 
 
 
 
 
 # Define the township and file name
 file_name <- paste0(township, "10.shp")
 
 # Specify the directory path
 directory <- "Base Data"
 
 # Create the complete file path
 output_file <- file.path(directory, file_name)
 
 # Create an sf object with your data
 sf_obj <- final10  # Replace `your_sf_object` with your actual sf object
 
 sf_obj <- sf_obj %>%
   select(pin10, result, geometry)
 
 # Save the complete shapefile
 sf::st_write(sf_obj, output_file, append = FALSE)
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 # Parcel data
 parcels <- st_read(
   glue::glue(
     "https://datacatalog.cookcountyil.gov/resource/77tz-riq7.geojson?PoliticalTownship=Town%20of%20{township}&$limit=1000000"
   )) %>%
   mutate(id = row_number())
 
 
 
 parcels <- parcels %>%
   slice(50001: 55000)
 
 
 
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
 
 
 # dan_func_single_obs(9, parcels %>% st_transform(3435), parcels_buffered, network_trans)
 
 # 
 result <- numeric(nrow(parcels))
 # 
 for (x in 1:nrow(parcels)) {
   result[x] <- dan_func_single_obs(x, parcels %>% st_transform(3435), parcels_buffered, network_trans)
   cat("Iteration:", x, "/", nrow(parcels), "\n")  # Print iteration progress
 }
 
 
 
 final11 <- cbind(parcels, result)
 table(final11$result)
 
 
 ggplot() +
   geom_sf(data = parcels) +
   geom_sf(data = st_as_sf(network, 'edges'), col = 'green')
 
 
 
 
 
 
 
 
 # Define the township and file name
 file_name <- paste0(township, "11.shp")
 
 # Specify the directory path
 directory <- "Base Data"
 
 # Create the complete file path
 output_file <- file.path(directory, file_name)
 
 # Create an sf object with your data
 sf_obj <- final11  # Replace `your_sf_object` with your actual sf object
 
 sf_obj <- sf_obj %>%
   select(pin10, result, geometry)
 
 # Save the complete shapefile
 sf::st_write(sf_obj, output_file, append = FALSE)
 
 
 
 
 
 
 
 
 # Parcel data
 parcels <- st_read(
   glue::glue(
     "https://datacatalog.cookcountyil.gov/resource/77tz-riq7.geojson?PoliticalTownship=Town%20of%20{township}&$limit=1000000"
   )) %>%
   mutate(id = row_number())
 
 
 
 parcels <- parcels %>%
   slice(55001: 60000)
 
 
 
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
 
 
 # dan_func_single_obs(9, parcels %>% st_transform(3435), parcels_buffered, network_trans)
 
 # 
 result <- numeric(nrow(parcels))
 # 
 for (x in 1:nrow(parcels)) {
   result[x] <- dan_func_single_obs(x, parcels %>% st_transform(3435), parcels_buffered, network_trans)
   cat("Iteration:", x, "/", nrow(parcels), "\n")  # Print iteration progress
 }
 
 
 
 final12 <- cbind(parcels, result)
 
 
 
 ggplot() +
   geom_sf(data = parcels) +
   geom_sf(data = st_as_sf(network, 'edges'), col = 'green')
 
 
 
 
 
 
 
 
 # Define the township and file name
 file_name <- paste0(township, "12.shp")
 
 # Specify the directory path
 directory <- "Base Data"
 
 # Create the complete file path
 output_file <- file.path(directory, file_name)
 
 # Create an sf object with your data
 sf_obj <- final12  # Replace `your_sf_object` with your actual sf object
 
 sf_obj <- sf_obj %>%
   select(pin10, result, geometry)
 
 # Save the complete shapefile
 sf::st_write(sf_obj, output_file, append = FALSE)
 
 


 
 
 
 
 
 
 
 # Parcel data
 parcels <- st_read(
   glue::glue(
     "https://datacatalog.cookcountyil.gov/resource/77tz-riq7.geojson?PoliticalTownship=Town%20of%20{township}&$limit=1000000"
   )) %>%
   mutate(id = row_number())
 
 
 
 parcels <- parcels %>%
   slice(60001: 65000)
 
 
 
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
 
 
 # dan_func_single_obs(9, parcels %>% st_transform(3435), parcels_buffered, network_trans)
 
 # 
 result <- numeric(nrow(parcels))
 # 
 for (x in 1:nrow(parcels)) {
   result[x] <- dan_func_single_obs(x, parcels %>% st_transform(3435), parcels_buffered, network_trans)
   cat("Iteration:", x, "/", nrow(parcels), "\n")  # Print iteration progress
 }
 
 
 
 final13 <- cbind(parcels, result)
 
 
 
 ggplot() +
   geom_sf(data = parcels) +
   geom_sf(data = st_as_sf(network, 'edges'), col = 'green')
 
 
 
 
 
 
 
 
 # Define the township and file name
 file_name <- paste0(township, "13.shp")
 
 # Specify the directory path
 directory <- "Base Data"
 
 # Create the complete file path
 output_file <- file.path(directory, file_name)
 
 # Create an sf object with your data
 sf_obj <- final13  # Replace `your_sf_object` with your actual sf object
 
 sf_obj <- sf_obj %>%
   select(pin10, result, geometry)
 
 # Save the complete shapefile
 sf::st_write(sf_obj, output_file, append = FALSE)
 
 
 
 
 
 
 
 
 
 
 # Parcel data
 parcels <- st_read(
   glue::glue(
     "https://datacatalog.cookcountyil.gov/resource/77tz-riq7.geojson?PoliticalTownship=Town%20of%20{township}&$limit=1000000"
   )) %>%
   mutate(id = row_number())
 
 
 
 parcels <- parcels %>%
   slice(65001: 70000)
 
 
 
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
 
 
 # dan_func_single_obs(9, parcels %>% st_transform(3435), parcels_buffered, network_trans)
 
 # 
 result <- numeric(nrow(parcels))
 # 
 for (x in 1:nrow(parcels)) {
   result[x] <- dan_func_single_obs(x, parcels %>% st_transform(3435), parcels_buffered, network_trans)
   cat("Iteration:", x, "/", nrow(parcels), "\n")  # Print iteration progress
 }
 
 
 
 final14 <- cbind(parcels, result)
 
 
 
 ggplot() +
   geom_sf(data = parcels) +
   geom_sf(data = st_as_sf(network, 'edges'), col = 'green')
 
 
 
 
 
 
 
 
 # Define the township and file name
 file_name <- paste0(township, "14.shp")
 
 # Specify the directory path
 directory <- "Base Data"
 
 # Create the complete file path
 output_file <- file.path(directory, file_name)
 
 # Create an sf object with your data
 sf_obj <- final14  # Replace `your_sf_object` with your actual sf object
 
 sf_obj <- sf_obj %>%
   select(pin10, result, geometry)
 
 # Save the complete shapefile
 sf::st_write(sf_obj, output_file, append = FALSE)
 
 
 
 
 
 
 
 
 # Parcel data
 parcels <- st_read(
   glue::glue(
     "https://datacatalog.cookcountyil.gov/resource/77tz-riq7.geojson?PoliticalTownship=Town%20of%20{township}&$limit=1000000"
   )) %>%
   mutate(id = row_number())
 
 
 
 parcels <- parcels %>%
   slice(70001: 75000)
 
 
 
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
 
 
 # dan_func_single_obs(9, parcels %>% st_transform(3435), parcels_buffered, network_trans)
 
 # 
 result <- numeric(nrow(parcels))
 # 
 for (x in 1:nrow(parcels)) {
   result[x] <- dan_func_single_obs(x, parcels %>% st_transform(3435), parcels_buffered, network_trans)
   cat("Iteration:", x, "/", nrow(parcels), "\n")  # Print iteration progress
 }
 
 
 
 final15 <- cbind(parcels, result)
 
 
 
 ggplot() +
   geom_sf(data = parcels) +
   geom_sf(data = st_as_sf(network, 'edges'), col = 'green')
 
 
 
 
 
 
 
 
 # Define the township and file name
 file_name <- paste0(township, "15.shp")
 
 # Specify the directory path
 directory <- "Base Data"
 
 # Create the complete file path
 output_file <- file.path(directory, file_name)
 
 # Create an sf object with your data
 sf_obj <- final15  # Replace `your_sf_object` with your actual sf object
 
 sf_obj <- sf_obj %>%
   select(pin10, result, geometry)
 
 # Save the complete shapefile
 sf::st_write(sf_obj, output_file, append = FALSE)
 
 
 
 
 
 # Parcel data
 parcels <- st_read(
   glue::glue(
     "https://datacatalog.cookcountyil.gov/resource/77tz-riq7.geojson?PoliticalTownship=Town%20of%20{township}&$limit=1000000"
   )) %>%
   mutate(id = row_number())
 
 
 
 parcels <- parcels %>%
   slice(75001: 80000)
 
 
 
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
 
 
 # dan_func_single_obs(9, parcels %>% st_transform(3435), parcels_buffered, network_trans)
 
 # 
 result <- numeric(nrow(parcels))
 # 
 for (x in 1:nrow(parcels)) {
   result[x] <- dan_func_single_obs(x, parcels %>% st_transform(3435), parcels_buffered, network_trans)
   cat("Iteration:", x, "/", nrow(parcels), "\n")  # Print iteration progress
 }
 
 
 
 final16 <- cbind(parcels, result)
 
 
 
 ggplot() +
   geom_sf(data = parcels) +
   geom_sf(data = st_as_sf(network, 'edges'), col = 'green')
 
 
 
 
 
 
 
 
 # Define the township and file name
 file_name <- paste0(township, "16.shp")
 
 # Specify the directory path
 directory <- "Base Data"
 
 # Create the complete file path
 output_file <- file.path(directory, file_name)
 
 # Create an sf object with your data
 sf_obj <- final16  # Replace `your_sf_object` with your actual sf object
 
 sf_obj <- sf_obj %>%
   select(pin10, result, geometry)
 
 # Save the complete shapefile
 sf::st_write(sf_obj, output_file, append = FALSE)
 
 
 
 
 
 
 
 
 
 
 
 
 # Parcel data
 parcels <- st_read(
   glue::glue(
     "https://datacatalog.cookcountyil.gov/resource/77tz-riq7.geojson?PoliticalTownship=Town%20of%20{township}&$limit=1000000"
   )) %>%
   mutate(id = row_number())
 
 
 
 parcels <- parcels %>%
   slice(80001: 85000)
 
 
 
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
 
 
 # dan_func_single_obs(9, parcels %>% st_transform(3435), parcels_buffered, network_trans)
 
 # 
 result <- numeric(nrow(parcels))
 # 
 for (x in 1:nrow(parcels)) {
   result[x] <- dan_func_single_obs(x, parcels %>% st_transform(3435), parcels_buffered, network_trans)
   cat("Iteration:", x, "/", nrow(parcels), "\n")  # Print iteration progress
 }
 
 
 
 final17 <- cbind(parcels, result)
 
 
 
 ggplot() +
   geom_sf(data = parcels) +
   geom_sf(data = st_as_sf(network, 'edges'), col = 'green')
 
 
 
 
 
 
 
 
 # Define the township and file name
 file_name <- paste0(township, "17.shp")
 
 # Specify the directory path
 directory <- "Base Data"
 
 # Create the complete file path
 output_file <- file.path(directory, file_name)
 
 # Create an sf object with your data
 sf_obj <- final17  # Replace `your_sf_object` with your actual sf object
 
 sf_obj <- sf_obj %>%
   select(pin10, result, geometry)
 
 # Save the complete shapefile
 sf::st_write(sf_obj, output_file, append = FALSE)
 
 
 
 
 
 
 
 
 
 
 
 # Parcel data
 parcels <- st_read(
   glue::glue(
     "https://datacatalog.cookcountyil.gov/resource/77tz-riq7.geojson?PoliticalTownship=Town%20of%20{township}&$limit=1000000"
   )) %>%
   mutate(id = row_number())
 
 
 
 parcels <- parcels %>%
   slice(85001: 90000)
 
 
 
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
 
 
 # dan_func_single_obs(9, parcels %>% st_transform(3435), parcels_buffered, network_trans)
 
 # 
 result <- numeric(nrow(parcels))
 # 
 for (x in 1:nrow(parcels)) {
   result[x] <- dan_func_single_obs(x, parcels %>% st_transform(3435), parcels_buffered, network_trans)
   cat("Iteration:", x, "/", nrow(parcels), "\n")  # Print iteration progress
 }
 
 
 
 final18 <- cbind(parcels, result)
 
 
 
 ggplot() +
   geom_sf(data = parcels) +
   geom_sf(data = st_as_sf(network, 'edges'), col = 'green')
 
 
 
 
 
 
 
 
 # Define the township and file name
 file_name <- paste0(township, "18.shp")
 
 # Specify the directory path
 directory <- "Base Data"
 
 # Create the complete file path
 output_file <- file.path(directory, file_name)
 
 # Create an sf object with your data
 sf_obj <- final18  # Replace `your_sf_object` with your actual sf object
 
 sf_obj <- sf_obj %>%
   select(pin10, result, geometry)
 
 # Save the complete shapefile
 sf::st_write(sf_obj, output_file, append = FALSE)
 
 
 
 
 
 
 
 
 # Parcel data
 parcels <- st_read(
   glue::glue(
     "https://datacatalog.cookcountyil.gov/resource/77tz-riq7.geojson?PoliticalTownship=Town%20of%20{township}&$limit=1000000"
   )) %>%
   mutate(id = row_number())
 
 
 
 parcels <- parcels %>%
   slice(90001: 95000)
 
 
 
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
 
 
 # dan_func_single_obs(9, parcels %>% st_transform(3435), parcels_buffered, network_trans)
 
 # 
 result <- numeric(nrow(parcels))
 # 
 for (x in 1:nrow(parcels)) {
   result[x] <- dan_func_single_obs(x, parcels %>% st_transform(3435), parcels_buffered, network_trans)
   cat("Iteration:", x, "/", nrow(parcels), "\n")  # Print iteration progress
 }
 
 
 
 final19 <- cbind(parcels, result)
 
 
 
 ggplot() +
   geom_sf(data = parcels) +
   geom_sf(data = st_as_sf(network, 'edges'), col = 'green')
 
 
 
 
 
 
 
 
 # Define the township and file name
 file_name <- paste0(township, "19.shp")
 
 # Specify the directory path
 directory <- "Base Data"
 
 # Create the complete file path
 output_file <- file.path(directory, file_name)
 
 # Create an sf object with your data
 sf_obj <- final19  # Replace `your_sf_object` with your actual sf object
 
 sf_obj <- sf_obj %>%
   select(pin10, result, geometry)
 
 # Save the complete shapefile
 sf::st_write(sf_obj, output_file, append = FALSE)
 
 
 
 
 
 
 
 # Parcel data
 parcels <- st_read(
   glue::glue(
     "https://datacatalog.cookcountyil.gov/resource/77tz-riq7.geojson?PoliticalTownship=Town%20of%20{township}&$limit=1000000"
   )) %>%
   mutate(id = row_number())
 
 
 
 parcels <- parcels %>%
   slice(95001: 100000)
 
 
 
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
 
 
 # dan_func_single_obs(9, parcels %>% st_transform(3435), parcels_buffered, network_trans)
 
 # 
 result <- numeric(nrow(parcels))
 # 
 for (x in 1:nrow(parcels)) {
   result[x] <- dan_func_single_obs(x, parcels %>% st_transform(3435), parcels_buffered, network_trans)
   cat("Iteration:", x, "/", nrow(parcels), "\n")  # Print iteration progress
 }
 
 
 
 final20 <- cbind(parcels, result)
 
 
 
 ggplot() +
   geom_sf(data = parcels) +
   geom_sf(data = st_as_sf(network, 'edges'), col = 'green')
 
 
 
 
 
 
 
 
 # Define the township and file name
 file_name <- paste0(township, "20.shp")
 
 # Specify the directory path
 directory <- "Base Data"
 
 # Create the complete file path
 output_file <- file.path(directory, file_name)
 
 # Create an sf object with your data
 sf_obj <- final20  # Replace `your_sf_object` with your actual sf object
 
 sf_obj <- sf_obj %>%
   select(pin10, result, geometry)
 
 # Save the complete shapefile
 sf::st_write(sf_obj, output_file, append = FALSE)
 
 
 
 
 
 
 
 
 # Parcel data
 parcels <- st_read(
   glue::glue(
     "https://datacatalog.cookcountyil.gov/resource/77tz-riq7.geojson?PoliticalTownship=Town%20of%20{township}&$limit=1000000"
   )) %>%
   mutate(id = row_number())
 
 
 
 parcels <- parcels %>%
   slice(100001: 105000)
 
 
 
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
 
 
 # dan_func_single_obs(9, parcels %>% st_transform(3435), parcels_buffered, network_trans)
 
 # 
 result <- numeric(nrow(parcels))
 # 
 for (x in 1:nrow(parcels)) {
   result[x] <- dan_func_single_obs(x, parcels %>% st_transform(3435), parcels_buffered, network_trans)
   cat("Iteration:", x, "/", nrow(parcels), "\n")  # Print iteration progress
 }
 
 
 
 final21 <- cbind(parcels, result)
 
 
 
 ggplot() +
   geom_sf(data = parcels) +
   geom_sf(data = st_as_sf(network, 'edges'), col = 'green')
 
 
 
 
 
 
 
 
 # Define the township and file name
 file_name <- paste0(township, "21.shp")
 
 # Specify the directory path
 directory <- "Base Data"
 
 # Create the complete file path
 output_file <- file.path(directory, file_name)
 
 # Create an sf object with your data
 sf_obj <- final21  # Replace `your_sf_object` with your actual sf object
 
 sf_obj <- sf_obj %>%
   select(pin10, result, geometry)
 
 # Save the complete shapefile
 sf::st_write(sf_obj, output_file, append = FALSE)
 
 
 
 
 
 
 
 # Parcel data
 parcels <- st_read(
   glue::glue(
     "https://datacatalog.cookcountyil.gov/resource/77tz-riq7.geojson?PoliticalTownship=Town%20of%20{township}&$limit=1000000"
   )) %>%
   mutate(id = row_number())
 
 
 
 parcels <- parcels %>%
   slice(105001: 110000)
 
 
 
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
 
 
 # dan_func_single_obs(9, parcels %>% st_transform(3435), parcels_buffered, network_trans)
 
 # 
 result <- numeric(nrow(parcels))
 # 
 for (x in 1:nrow(parcels)) {
   result[x] <- dan_func_single_obs(x, parcels %>% st_transform(3435), parcels_buffered, network_trans)
   cat("Iteration:", x, "/", nrow(parcels), "\n")  # Print iteration progress
 }
 
 
 
 final22 <- cbind(parcels, result)
 
 
 
 ggplot() +
   geom_sf(data = parcels) +
   geom_sf(data = st_as_sf(network, 'edges'), col = 'green')
 
 
 
 
 
 
 
 
 # Define the township and file name
 file_name <- paste0(township, "22.shp")
 
 # Specify the directory path
 directory <- "Base Data"
 
 # Create the complete file path
 output_file <- file.path(directory, file_name)
 
 # Create an sf object with your data
 sf_obj <- final22  # Replace `your_sf_object` with your actual sf object
 
 sf_obj <- sf_obj %>%
   select(pin10, result, geometry)
 
 # Save the complete shapefile
 sf::st_write(sf_obj, output_file, append = FALSE)
 
 
 
 
 
 
 
 
 # Parcel data
 parcels <- st_read(
   glue::glue(
     "https://datacatalog.cookcountyil.gov/resource/77tz-riq7.geojson?PoliticalTownship=Town%20of%20{township}&$limit=1000000"
   )) %>%
   mutate(id = row_number())
 
 
 
 parcels <- parcels %>%
   slice(110001: 115000)
 
 
 
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
 
 
 # dan_func_single_obs(9, parcels %>% st_transform(3435), parcels_buffered, network_trans)
 
 # 
 result <- numeric(nrow(parcels))
 # 
 for (x in 1:nrow(parcels)) {
   result[x] <- dan_func_single_obs(x, parcels %>% st_transform(3435), parcels_buffered, network_trans)
   cat("Iteration:", x, "/", nrow(parcels), "\n")  # Print iteration progress
 }
 
 
 
 final23 <- cbind(parcels, result)
 
 
 
 ggplot() +
   geom_sf(data = parcels) +
   geom_sf(data = st_as_sf(network, 'edges'), col = 'green')
 
 
 
 
 
 
 
 
 # Define the township and file name
 file_name <- paste0(township, "23.shp")
 
 # Specify the directory path
 directory <- "Base Data"
 
 # Create the complete file path
 output_file <- file.path(directory, file_name)
 
 # Create an sf object with your data
 sf_obj <- final23  # Replace `your_sf_object` with your actual sf object
 
 sf_obj <- sf_obj %>%
   select(pin10, result, geometry)
 
 # Save the complete shapefile
 sf::st_write(sf_obj, output_file, append = FALSE)
 
 
 
 
 
 
 
 
 # Parcel data
 parcels <- st_read(
   glue::glue(
     "https://datacatalog.cookcountyil.gov/resource/77tz-riq7.geojson?PoliticalTownship=Town%20of%20{township}&$limit=1000000"
   )) %>%
   mutate(id = row_number())
 
 
 
 parcels <- parcels %>%
   slice(115001: 120000)
 
 
 
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
 
 
 # dan_func_single_obs(9, parcels %>% st_transform(3435), parcels_buffered, network_trans)
 
 # 
 result <- numeric(nrow(parcels))
 # 
 for (x in 1:nrow(parcels)) {
   result[x] <- dan_func_single_obs(x, parcels %>% st_transform(3435), parcels_buffered, network_trans)
   cat("Iteration:", x, "/", nrow(parcels), "\n")  # Print iteration progress
 }
 
 
 
 final24 <- cbind(parcels, result)
 
 
 
 ggplot() +
   geom_sf(data = parcels) +
   geom_sf(data = st_as_sf(network, 'edges'), col = 'green')
 
 
 
 
 
 
 
 
 # Define the township and file name
 file_name <- paste0(township, "24.shp")
 
 # Specify the directory path
 directory <- "Base Data"
 
 # Create the complete file path
 output_file <- file.path(directory, file_name)
 
 # Create an sf object with your data
 sf_obj <- final24  # Replace `your_sf_object` with your actual sf object
 
 sf_obj <- sf_obj %>%
   select(pin10, result, geometry)
 
 # Save the complete shapefile
 sf::st_write(sf_obj, output_file, append = FALSE)
 
 
 
 
 
 # Parcel data
 parcels <- st_read(
   glue::glue(
     "https://datacatalog.cookcountyil.gov/resource/77tz-riq7.geojson?PoliticalTownship=Town%20of%20{township}&$limit=1000000"
   )) %>%
   mutate(id = row_number())
 
 
 parcels <- parcels %>%
   slice(120001: 125000)
 
 
 
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
 
 
 # dan_func_single_obs(9, parcels %>% st_transform(3435), parcels_buffered, network_trans)
 
 # 
 result <- numeric(nrow(parcels))
 # 
 for (x in 1:nrow(parcels)) {
   result[x] <- dan_func_single_obs(x, parcels %>% st_transform(3435), parcels_buffered, network_trans)
   cat("Iteration:", x, "/", nrow(parcels), "\n")  # Print iteration progress
 }
 
 
 
 final25 <- cbind(parcels, result)
 
 
 
 ggplot() +
   geom_sf(data = parcels) +
   geom_sf(data = st_as_sf(network, 'edges'), col = 'green')
 
 
 
 
 
 
 
 
 # Define the township and file name
 file_name <- paste0(township, "25.shp")
 
 # Specify the directory path
 directory <- "Base Data"
 
 # Create the complete file path
 output_file <- file.path(directory, file_name)
 
 # Create an sf object with your data
 sf_obj <- final25  # Replace `your_sf_object` with your actual sf object
 
 sf_obj <- sf_obj %>%
   select(pin10, result, geometry)
 
 # Save the complete shapefile
 sf::st_write(sf_obj, output_file, append = FALSE)
 
 
 
 
 # Parcel data
 parcels <- st_read(
   glue::glue(
     "https://datacatalog.cookcountyil.gov/resource/77tz-riq7.geojson?PoliticalTownship=Town%20of%20{township}&$limit=1000000"
   )) %>%
   mutate(id = row_number())
 
 
 
 parcels <- parcels %>%
   slice(125001: 130000)
 
 
 
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
 
 
 # dan_func_single_obs(9, parcels %>% st_transform(3435), parcels_buffered, network_trans)
 
 # 
 result <- numeric(nrow(parcels))
 # 
 for (x in 1:nrow(parcels)) {
   result[x] <- dan_func_single_obs(x, parcels %>% st_transform(3435), parcels_buffered, network_trans)
   cat("Iteration:", x, "/", nrow(parcels), "\n")  # Print iteration progress
 }
 
 
 
 final26 <- cbind(parcels, result)
 
 
 
 ggplot() +
   geom_sf(data = parcels) +
   geom_sf(data = st_as_sf(network, 'edges'), col = 'green')
 
 
 
 
 
 
 
 
 # Define the township and file name
 file_name <- paste0(township, "26.shp")
 
 # Specify the directory path
 directory <- "Base Data"
 
 # Create the complete file path
 output_file <- file.path(directory, file_name)
 
 # Create an sf object with your data
 sf_obj <- final26  # Replace `your_sf_object` with your actual sf object
 
 sf_obj <- sf_obj %>%
   select(pin10, result, geometry)
 
 # Save the complete shapefile
 sf::st_write(sf_obj, output_file, append = FALSE)
 
 
 
 
 
 
 
 # Parcel data
 parcels <- st_read(
   glue::glue(
     "https://datacatalog.cookcountyil.gov/resource/77tz-riq7.geojson?PoliticalTownship=Town%20of%20{township}&$limit=1000000"
   )) %>%
   mutate(id = row_number())
 
 
 
 parcels <- parcels %>%
   slice(130001: 135000)
 
 
 
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
 
 
 # dan_func_single_obs(9, parcels %>% st_transform(3435), parcels_buffered, network_trans)
 
 # 
 result <- numeric(nrow(parcels))
 # 
 for (x in 1:nrow(parcels)) {
   result[x] <- dan_func_single_obs(x, parcels %>% st_transform(3435), parcels_buffered, network_trans)
   cat("Iteration:", x, "/", nrow(parcels), "\n")  # Print iteration progress
 }
 
 
 
 final27 <- cbind(parcels, result)
 
 
 
 ggplot() +
   geom_sf(data = final27,  aes(fill = result)) +
   geom_sf(data = st_as_sf(network, 'edges'), col = 'green')
 
 
 
 
 
 
 
 
 # Define the township and file name
 file_name <- paste0(township, "Final.shp")
 
 # Specify the directory path
 directory <- "Base Data"
 
 # Create the complete file path
 output_file <- file.path(directory, file_name)
 
 # Create an sf object with your data
 sf_obj <- Lakefinal  # Replace `your_sf_object` with your actual sf object
 
 sf_obj <- sf_obj %>%
   select(pin10, result, geometry)
 
 # Save the complete shapefile
 sf::st_write(sf_obj, output_file, append = FALSE)
 
 
 
 Lakefinal <- rbind(final1, final2, final3, final4, final5, final6, final7, final8, final9, final10, final11, final12, final13, final14, final15, final16, final17, final18, final19, final20, final21, final22, final23, final24, final25, final26, final27, final28, final29, final30, final31, final32, final33, final34, final35, final36, final37, final38)

 
 
 
 
 
 
 
 # Parcel data
 parcels <- st_read(
   glue::glue(
     "https://datacatalog.cookcountyil.gov/resource/77tz-riq7.geojson?PoliticalTownship=Town%20of%20{township}&$limit=1000000"
   )) %>%
   mutate(id = row_number())
 
 
 
 parcels <- parcels %>%
   slice(135001: 140000)
 
 
 
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
 
 
 # dan_func_single_obs(9, parcels %>% st_transform(3435), parcels_buffered, network_trans)
 
 # 
 result <- numeric(nrow(parcels))
 # 
 for (x in 1:nrow(parcels)) {
   result[x] <- dan_func_single_obs(x, parcels %>% st_transform(3435), parcels_buffered, network_trans)
   cat("Iteration:", x, "/", nrow(parcels), "\n")  # Print iteration progress
 }
 
 
 
 final28 <- cbind(parcels, result)
 
 
 
 ggplot() +
   geom_sf(data = parcels) +
   geom_sf(data = st_as_sf(network, 'edges'), col = 'green')
 
 
 
 
 
 
 
 
 # Define the township and file name
 file_name <- paste0(township, "28.shp")
 
 # Specify the directory path
 directory <- "Base Data"
 
 # Create the complete file path
 output_file <- file.path(directory, file_name)
 
 # Create an sf object with your data
 sf_obj <- final28  # Replace `your_sf_object` with your actual sf object
 
 sf_obj <- sf_obj %>%
   select(pin10, result, geometry)
 
 # Save the complete shapefile
 sf::st_write(sf_obj, output_file, append = FALSE)
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 # Parcel data
 parcels <- st_read(
   glue::glue(
     "https://datacatalog.cookcountyil.gov/resource/77tz-riq7.geojson?PoliticalTownship=Town%20of%20{township}&$limit=1000000"
   )) %>%
   mutate(id = row_number())
 
 
 
 parcels <- parcels %>%
   slice(140001: 145000)
 
 
 
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
 
 
 # dan_func_single_obs(9, parcels %>% st_transform(3435), parcels_buffered, network_trans)
 
 # 
 result <- numeric(nrow(parcels))
 # 
 for (x in 1:nrow(parcels)) {
   result[x] <- dan_func_single_obs(x, parcels %>% st_transform(3435), parcels_buffered, network_trans)
   cat("Iteration:", x, "/", nrow(parcels), "\n")  # Print iteration progress
 }
 
 
 
 final29 <- cbind(parcels, result)
 
 
 
 ggplot() +
   geom_sf(data = parcels) +
   geom_sf(data = st_as_sf(network, 'edges'), col = 'green')
 
 
 
 
 
 
 
 
 # Define the township and file name
 file_name <- paste0(township, "29.shp")
 
 # Specify the directory path
 directory <- "Base Data"
 
 # Create the complete file path
 output_file <- file.path(directory, file_name)
 
 # Create an sf object with your data
 sf_obj <- final29  # Replace `your_sf_object` with your actual sf object
 
 sf_obj <- sf_obj %>%
   select(pin10, result, geometry)
 
 # Save the complete shapefile
 sf::st_write(sf_obj, output_file, append = FALSE)
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 # Parcel data
 parcels <- st_read(
   glue::glue(
     "https://datacatalog.cookcountyil.gov/resource/77tz-riq7.geojson?PoliticalTownship=Town%20of%20{township}&$limit=1000000"
   )) %>%
   mutate(id = row_number())
 
 
 
 parcels <- parcels %>%
   slice(145001: 150000)
 
 
 
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
 
 
 # dan_func_single_obs(9, parcels %>% st_transform(3435), parcels_buffered, network_trans)
 
 # 
 result <- numeric(nrow(parcels))
 # 
 for (x in 1:nrow(parcels)) {
   result[x] <- dan_func_single_obs(x, parcels %>% st_transform(3435), parcels_buffered, network_trans)
   cat("Iteration:", x, "/", nrow(parcels), "\n")  # Print iteration progress
 }
 
 
 
 final30 <- cbind(parcels, result)
 
 
 
 ggplot() +
   geom_sf(data = parcels) +
   geom_sf(data = st_as_sf(network, 'edges'), col = 'green')
 
 
 
 
 
 
 
 
 # Define the township and file name
 file_name <- paste0(township, "30.shp")
 
 # Specify the directory path
 directory <- "Base Data"
 
 # Create the complete file path
 output_file <- file.path(directory, file_name)
 
 # Create an sf object with your data
 sf_obj <- final30  # Replace `your_sf_object` with your actual sf object
 
 sf_obj <- sf_obj %>%
   select(pin10, result, geometry)
 
 # Save the complete shapefile
 sf::st_write(sf_obj, output_file, append = FALSE)
 
 
 
 
 
 
 
 
 
 
 
 
 
 # Parcel data
 parcels <- st_read(
   glue::glue(
     "https://datacatalog.cookcountyil.gov/resource/77tz-riq7.geojson?PoliticalTownship=Town%20of%20{township}&$limit=1000000"
   )) %>%
   mutate(id = row_number())
 
 
 
 parcels <- parcels %>%
   slice(150001: 155000)
 
 
 
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
 
 
 # dan_func_single_obs(9, parcels %>% st_transform(3435), parcels_buffered, network_trans)
 
 # 
 result <- numeric(nrow(parcels))
 # 
 for (x in 1:nrow(parcels)) {
   result[x] <- dan_func_single_obs(x, parcels %>% st_transform(3435), parcels_buffered, network_trans)
   cat("Iteration:", x, "/", nrow(parcels), "\n")  # Print iteration progress
 }
 
 
 
 final31 <- cbind(parcels, result)
 
 
 
 ggplot() +
   geom_sf(data = parcels) +
   geom_sf(data = st_as_sf(network, 'edges'), col = 'green')
 
 
 
 
 
 
 
 
 # Define the township and file name
 file_name <- paste0(township, "31.shp")
 
 # Specify the directory path
 directory <- "Base Data"
 
 # Create the complete file path
 output_file <- file.path(directory, file_name)
 
 # Create an sf object with your data
 sf_obj <- final31  # Replace `your_sf_object` with your actual sf object
 
 sf_obj <- sf_obj %>%
   select(pin10, result, geometry)
 
 # Save the complete shapefile
 sf::st_write(sf_obj, output_file, append = FALSE)
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 # Parcel data
 parcels <- st_read(
   glue::glue(
     "https://datacatalog.cookcountyil.gov/resource/77tz-riq7.geojson?PoliticalTownship=Town%20of%20{township}&$limit=1000000"
   )) %>%
   mutate(id = row_number())
 
 
 
 parcels <- parcels %>%
   slice(155001: 160000)
 
 
 
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
 
 
 # dan_func_single_obs(9, parcels %>% st_transform(3435), parcels_buffered, network_trans)
 
 # 
 result <- numeric(nrow(parcels))
 # 
 for (x in 1:nrow(parcels)) {
   result[x] <- dan_func_single_obs(x, parcels %>% st_transform(3435), parcels_buffered, network_trans)
   cat("Iteration:", x, "/", nrow(parcels), "\n")  # Print iteration progress
 }
 
 
 
 final32 <- cbind(parcels, result)
 
 
 
 ggplot() +
   geom_sf(data = parcels) +
   geom_sf(data = st_as_sf(network, 'edges'), col = 'green')
 
 
 
 
 
 
 
 
 # Define the township and file name
 file_name <- paste0(township, "32.shp")
 
 # Specify the directory path
 directory <- "Base Data"
 
 # Create the complete file path
 output_file <- file.path(directory, file_name)
 
 # Create an sf object with your data
 sf_obj <- final32  # Replace `your_sf_object` with your actual sf object
 
 sf_obj <- sf_obj %>%
   select(pin10, result, geometry)
 
 # Save the complete shapefile
 sf::st_write(sf_obj, output_file, append = FALSE)
 
 
 
 
 
 
 
 
 
 
 
 
 
 # Parcel data
 parcels <- st_read(
   glue::glue(
     "https://datacatalog.cookcountyil.gov/resource/77tz-riq7.geojson?PoliticalTownship=Town%20of%20{township}&$limit=1000000"
   )) %>%
   mutate(id = row_number())
 
 
 
 parcels <- parcels %>%
   slice(160001: 165000)
 
 
 
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
 
 
 # dan_func_single_obs(9, parcels %>% st_transform(3435), parcels_buffered, network_trans)
 
 # 
 result <- numeric(nrow(parcels))
 # 
 for (x in 1:nrow(parcels)) {
   result[x] <- dan_func_single_obs(x, parcels %>% st_transform(3435), parcels_buffered, network_trans)
   cat("Iteration:", x, "/", nrow(parcels), "\n")  # Print iteration progress
 }
 
 
 
 final33 <- cbind(parcels, result)
 
 
 
 ggplot() +
   geom_sf(data = parcels) +
   geom_sf(data = st_as_sf(network, 'edges'), col = 'green')
 
 
 
 
 
 
 
 
 # Define the township and file name
 file_name <- paste0(township, "33.shp")
 
 # Specify the directory path
 directory <- "Base Data"
 
 # Create the complete file path
 output_file <- file.path(directory, file_name)
 
 # Create an sf object with your data
 sf_obj <- final33  # Replace `your_sf_object` with your actual sf object
 
 sf_obj <- sf_obj %>%
   select(pin10, result, geometry)
 
 # Save the complete shapefile
 sf::st_write(sf_obj, output_file, append = FALSE)
 
 
 
 
 
 
 
 
 
 
 # Parcel data
 parcels <- st_read(
   glue::glue(
     "https://datacatalog.cookcountyil.gov/resource/77tz-riq7.geojson?PoliticalTownship=Town%20of%20{township}&$limit=1000000"
   )) %>%
   mutate(id = row_number())
 
 
 
 parcels <- parcels %>%
   slice(165001: 170000)
 
 
 
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
 
 
 # dan_func_single_obs(9, parcels %>% st_transform(3435), parcels_buffered, network_trans)
 
 # 
 result <- numeric(nrow(parcels))
 # 
 for (x in 1:nrow(parcels)) {
   result[x] <- dan_func_single_obs(x, parcels %>% st_transform(3435), parcels_buffered, network_trans)
   cat("Iteration:", x, "/", nrow(parcels), "\n")  # Print iteration progress
 }
 
 
 
 final34 <- cbind(parcels, result)
 
 
 
 ggplot() +
   geom_sf(data = parcels) +
   geom_sf(data = st_as_sf(network, 'edges'), col = 'green')
 
 
 
 
 
 
 
 
 # Define the township and file name
 file_name <- paste0(township, "34.shp")
 
 # Specify the directory path
 directory <- "Base Data"
 
 # Create the complete file path
 output_file <- file.path(directory, file_name)
 
 # Create an sf object with your data
 sf_obj <- final34  # Replace `your_sf_object` with your actual sf object
 
 sf_obj <- sf_obj %>%
   select(pin10, result, geometry)
 
 # Save the complete shapefile
 sf::st_write(sf_obj, output_file, append = FALSE)
 
 
 
 
 
 
 
 
 
 # Parcel data
 parcels <- st_read(
   glue::glue(
     "https://datacatalog.cookcountyil.gov/resource/77tz-riq7.geojson?PoliticalTownship=Town%20of%20{township}&$limit=1000000"
   )) %>%
   mutate(id = row_number())
 
 
 
 parcels <- parcels %>%
   slice(170001: 175000)
 
 
 
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
 
 
 # dan_func_single_obs(9, parcels %>% st_transform(3435), parcels_buffered, network_trans)
 
 # 
 result <- numeric(nrow(parcels))
 # 
 for (x in 1:nrow(parcels)) {
   result[x] <- dan_func_single_obs(x, parcels %>% st_transform(3435), parcels_buffered, network_trans)
   cat("Iteration:", x, "/", nrow(parcels), "\n")  # Print iteration progress
 }
 
 
 
 final35 <- cbind(parcels, result)
 
 
 
 ggplot() +
   geom_sf(data = parcels) +
   geom_sf(data = st_as_sf(network, 'edges'), col = 'green')
 
 
 
 
 
 
 
 
 # Define the township and file name
 file_name <- paste0(township, "35.shp")
 
 # Specify the directory path
 directory <- "Base Data"
 
 # Create the complete file path
 output_file <- file.path(directory, file_name)
 
 # Create an sf object with your data
 sf_obj <- final35  # Replace `your_sf_object` with your actual sf object
 
 sf_obj <- sf_obj %>%
   select(pin10, result, geometry)
 
 # Save the complete shapefile
 sf::st_write(sf_obj, output_file, append = FALSE)
 
 
 
 
 
 
 
 
 
 
 
 
 # Parcel data
 parcels <- st_read(
   glue::glue(
     "https://datacatalog.cookcountyil.gov/resource/77tz-riq7.geojson?PoliticalTownship=Town%20of%20{township}&$limit=1000000"
   )) %>%
   mutate(id = row_number())
 
 
 
 parcels <- parcels %>%
   slice(175001: 180000)
 
 
 
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
 
 
 # dan_func_single_obs(9, parcels %>% st_transform(3435), parcels_buffered, network_trans)
 
 # 
 result <- numeric(nrow(parcels))
 # 
 for (x in 1:nrow(parcels)) {
   result[x] <- dan_func_single_obs(x, parcels %>% st_transform(3435), parcels_buffered, network_trans)
   cat("Iteration:", x, "/", nrow(parcels), "\n")  # Print iteration progress
 }
 
 
 
 final36 <- cbind(parcels, result)
 
 
 
 ggplot() +
   geom_sf(data = parcels) +
   geom_sf(data = st_as_sf(network, 'edges'), col = 'green')
 
 
 
 
 
 
 
 
 # Define the township and file name
 file_name <- paste0(township, "36.shp")
 
 # Specify the directory path
 directory <- "Base Data"
 
 # Create the complete file path
 output_file <- file.path(directory, file_name)
 
 # Create an sf object with your data
 sf_obj <- final36  # Replace `your_sf_object` with your actual sf object
 
 sf_obj <- sf_obj %>%
   select(pin10, result, geometry)
 
 # Save the complete shapefile
 sf::st_write(sf_obj, output_file, append = FALSE)
 
 
 
 
 
 
 
 
 # Parcel data
 parcels <- st_read(
   glue::glue(
     "https://datacatalog.cookcountyil.gov/resource/77tz-riq7.geojson?PoliticalTownship=Town%20of%20{township}&$limit=1000000"
   )) %>%
   mutate(id = row_number())
 
 
 
 parcels <- parcels %>%
   slice(180001: 185000)
 
 
 
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
 
 
 # dan_func_single_obs(9, parcels %>% st_transform(3435), parcels_buffered, network_trans)
 
 # 
 result <- numeric(nrow(parcels))
 # 
 for (x in 1:nrow(parcels)) {
   result[x] <- dan_func_single_obs(x, parcels %>% st_transform(3435), parcels_buffered, network_trans)
   cat("Iteration:", x, "/", nrow(parcels), "\n")  # Print iteration progress
 }
 
 
 
 final37 <- cbind(parcels, result)
 
 
 
 ggplot() +
   geom_sf(data = parcels) +
   geom_sf(data = st_as_sf(network, 'edges'), col = 'green')
 
 
 
 
 
 
 
 
 # Define the township and file name
 file_name <- paste0(township, "37.shp")
 
 # Specify the directory path
 directory <- "Base Data"
 
 # Create the complete file path
 output_file <- file.path(directory, file_name)
 
 # Create an sf object with your data
 sf_obj <- final37  # Replace `your_sf_object` with your actual sf object
 
 sf_obj <- sf_obj %>%
   select(pin10, result, geometry)
 
 # Save the complete shapefile
 sf::st_write(sf_obj, output_file, append = FALSE)
 
 
 
 
 
 
 
 
 
 
 # Parcel data
 parcels <- st_read(
   glue::glue(
     "https://datacatalog.cookcountyil.gov/resource/77tz-riq7.geojson?PoliticalTownship=Town%20of%20{township}&$limit=1000000"
   )) %>%
   mutate(id = row_number())
 
 
 
 parcels <- parcels %>%
   slice(185001: 188219)
 
 
 
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
 
 
 # dan_func_single_obs(9, parcels %>% st_transform(3435), parcels_buffered, network_trans)
 
 # 
 result <- numeric(nrow(parcels))
 # 
 for (x in 1:nrow(parcels)) {
   result[x] <- dan_func_single_obs(x, parcels %>% st_transform(3435), parcels_buffered, network_trans)
   cat("Iteration:", x, "/", nrow(parcels), "\n")  # Print iteration progress
 }
 
 
 
 final38 <- cbind(parcels, result)
 
 
 
 ggplot() +
   geom_sf(data = parcels) +
   geom_sf(data = st_as_sf(network, 'edges'), col = 'green')
 
 
 
 
 
 
 
 
 # Define the township and file name
 file_name <- paste0(township, "38.shp")
 
 # Specify the directory path
 directory <- "Base Data"
 
 # Create the complete file path
 output_file <- file.path(directory, file_name)
 
 # Create an sf object with your data
 sf_obj <- final38  # Replace `your_sf_object` with your actual sf object
 
 sf_obj <- sf_obj %>%
   select(pin10, result, geometry)
 
 # Save the complete shapefile
 sf::st_write(sf_obj, output_file, append = FALSE)