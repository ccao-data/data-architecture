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

# parcelsfull <- parcels



township <- "Calumet"

bbox <- ccao::town_shp %>%
 filter(township_name == township) %>%
  st_bbox()

# WORKS FOR -87.625 and doesn't for -87.4

# bbox <- st_bbox(c(xmin = -87.4, ymin = 41.642, xmax = -87.61, ymax = 41.8))



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

plot(town_osm_center[1])


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

# parcels <- parcels %>%
 #  filter(latitude >= 41.642 & latitude <= 41.8) %>% 
 #  filter(longitude <= -87.64 & longitude >= -87.61) 

parcel <- parcels$geometry

parcelsfull <- parcels






dan_func_single_obs <- function(x, parcels, network) {
  
  network_trans <- network %>%
    st_transform(4326)
  
  parcels_trans <- parcels %>%
    st_transform(3435)
  
  parcels_full <- parcels %>%
    st_transform(4326)
  
  # Create a nested list for target parcel, neighbor parcel, neighbor network
  # Find the neighbor parcel clip
  parcels_buffered <- parcels_trans %>%
    st_buffer(dist = units::set_units(20, "m")) %>%
    st_transform(4326)

  clip_network  <- network %>%
      activate("edges") %>%
      activate("nodes") %>%
      filter(!node_is_isolated()) %>%
      activate("edges") %>%
      st_as_sf() %>%
      st_transform(4326)

  
  
  # Step 1: Create the minimum rectangle
  min_rectangle <- parcels_trans[x, ] %>%
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
  centroid <- st_centroid(parcels_trans[x, ]) %>%
    st_as_sf() %>%
    st_transform(4326) %>%
    select(geometry)
  
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
    ) 
  
  
  # result <- list(
  #   rectangle_network = rectangle_network,
  #   cross_lst = split(cross, cross$id),
  #   clip_network = clip_network,
  #   clip_parcel = clip_parcel)
  
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
    filter((diff_degree >= 85 & diff_degree <= 95) | diff_degree <= 5 & diff_degree >= 375, na.rm = TRUE)
  

  touching_unit_street <- suppressWarnings(suppressMessages(st_intersects(cross$geometry, clip_network)))

  touching_street_number <- sum(lengths(touching_unit_street))
  
  cross_corner_number <- nrow(cross_corner)
  
  aspect_ratio <- max(cross$aspect_ratio, na.rm = TRUE)
  
  
  
  # TODO: Convert network to 4326
  # TODO: Buffered parcel has small buffer/or is touching
  # TODO: check logical criteria
  
  if (all(touching_street_number >= 2) & all(cross_corner_number >= 1) & all(aspect_ratio < 30)) {
    return(TRUE)
  } else {
    return(FALSE)
  }
}


#result <- numeric(nrow(parcels))
#for (x in 1:nrow(parcels)) {
#  result[x] <- dan_func_single_obs(x, parcels, network)
#}



dan_func_single_obs(8, parcels, network)

plot(parcels[8:12,])

table(result)

pin <- parcels %>%
  slice(1:30) %>%
  select(pin10)




# final <- cbind(pin, result) %>%
#   as.tibble %>%
#   select(1, 2)
#   

  
view(final)

corner_parcel_calumet <- parcels %>%
   mutate(corner_indicator = unlist(result)) %>%
   filter(corner_indicator == TRUE)

plot <- ggplot() +
  geom_sf(data = final) +
  geom_sf(data = st_as_sf(network, 'edges'), col = 'green') +
  coord_sf(xlim = c(-87.64,  -87.62), ylim = c( 41.642,  41.65), expand = FALSE)


plot(final[2])

for (i in 1:length(network)) {
  plot(network[[i]], add = TRUE)
}

table(final)